import com.google.gson.Gson;
import com.rabbitmq.client.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LiftRideGetConsumer {
    private static final String GET_QUEUE_NAME = "skiersGetQueue";
    private static final int RMQ_CHANNEL_POOL_SIZE = 100;
    private static final int NUM_CONSUMER_THREADS = 600;
    private static final int BATCH_SIZE = 25;  // Number of messages per Redis batch
    private static final int MAX_RETRIES = 5;  // Retry attempts for Redis operations

    // DynamoDB constants
    private static final String TABLE_NAME = "SkierNewTable";
    private static final String PartitionKey = "PK";
    private static final String SortKey = "SK";

    // RabbitMQ constants
    private static final String GET_RESPONSE_QUEUE_NAME = "skiersGetResponseQueue";
    private static final String GET_TOTAL_DAY_VERTICAL_MESSAGE_KEY = "GET_DAY_VERTICAL";
    private static final String GET_TOTAL_RESORT_VERTICAL_MESSAGE_KEY = "GET_RESORT_VERTICAL";
    private static final String GET_NUM_UNIQUE_SKIERS_MESSAGE_KEY = "GET_NUM_UNIQUE_SKIERS";

    // Connections
    private Connection connection;
    private RMQChannelPool channelPool;
    private JedisPool jedisPool; // Redis connection pool
    private DynamoDbClient dynamoDbClient;
    private final Gson gson = new Gson();


    public static void main(String[] args) {
        new LiftRideGetConsumer().startConsuming();
    }

    public void startConsuming() {
        // Set up ExecutorService for multiple consumers
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMER_THREADS);

        try {
            // Set up RabbitMQ connection
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(Config.getRMQHost()); // Adjust host as necessary
            factory.setUsername(Config.getRMQUsername());
            factory.setPassword(Config.getRMQPassword());
            factory.setPort(Config.getRMQPort());
            connection = factory.newConnection();

            channelPool = new RMQChannelPool(RMQ_CHANNEL_POOL_SIZE, new RMQChannelFactory(connection));
            jedisPool = RedisConnectionManager.getJedisPool();

            // Initialize DynamoDB client
            Region region = Region.US_WEST_2;
            dynamoDbClient = DynamoDbClient.builder()
                    .region(region)
                    .build();

            // Start consumer threads
            for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
                executor.submit(this::consumeMessages);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void consumeMessages() {
        try {
            Channel channel = channelPool.borrowObject();
            channel.basicConsume(GET_QUEUE_NAME, true, (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                Map<String, Object> request = gson.fromJson(message, Map.class);
                String response = processGetRequest(request);

                String correlationId = delivery.getProperties().getCorrelationId();
                String replyTo = delivery.getProperties().getReplyTo();  // Dynamic reply queue

                // Ensure we have a valid replyTo queue
                if (replyTo != null && !replyTo.isEmpty()) {
                    try {
                        channel.basicPublish(
                                "",
                                replyTo,
                                new AMQP.BasicProperties.Builder()
                                        .correlationId(correlationId)
                                        .build(),
                                response.getBytes(StandardCharsets.UTF_8)
                        );
                    } catch (Exception e) {
                        System.err.println("Error sending response to dynamic reply queue: " + e.getMessage());
                    }
                } else {
                    System.err.println("No replyTo queue specified. Unable to send response.");
                }
            }, consumerTag -> {
                System.out.println("Consumer " + consumerTag + " canceled");
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    private String processGetRequest(Map<String, Object> request) {
        try {
            String type = (String) request.get("type");
//            System.out.println("Request type: " + type);
            if (GET_TOTAL_DAY_VERTICAL_MESSAGE_KEY.equals(type)) {
                return getDayVertical(request);
            }
            else if (GET_TOTAL_RESORT_VERTICAL_MESSAGE_KEY.equals(type)) {
                return getResortVertical(request);
            }
            else if (GET_NUM_UNIQUE_SKIERS_MESSAGE_KEY.equals(type)) {
                return getNumUniqueSkiers(request);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson(Map.of(
                    "response_code", 500,
                    "message", "Internal error processing GET request"
            ));
        }
        return "{\"message\":\"Failed to process GET request\"}";
    }

    // API 1: /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
    private String getNumUniqueSkiers(Map<String, Object> request) {
        String resortID = (String) request.get("resortID");
        String seasonID = (String) request.get("seasonID");
        String dayID = (String) request.get("dayID");

        String gsiPK = "RESORT#" + resortID + "#SEASON#" + seasonID + "#DAY#" + dayID;
        String redisKey = "uniqueSkiers:" + gsiPK; // Redis cache key

        try (Jedis jedis = jedisPool.getResource()) {
            // Check Redis for cached data
            String cachedResult = jedis.get(redisKey);
            if (cachedResult != null) {
                return cachedResult; // Return cached result
            }

            // Cache miss: Query DynamoDB
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName(TABLE_NAME)
                    .indexName("GSI_PK-index")
                    .keyConditionExpression("GSI_PK = :gsiPK")
                    .projectionExpression("PK")
                    .expressionAttributeValues(Map.of(
                            ":gsiPK", AttributeValue.builder().s(gsiPK).build()
                    ))
                    .build();

            QueryResponse result = dynamoDbClient.query(queryRequest);

            // Use a HashSet for distinct PK counting
            Set<String> uniqueSkiers = new HashSet<>();
            for (Map<String, AttributeValue> item : result.items()) {
                AttributeValue pkAttr = item.get("PK");
                if (pkAttr != null && pkAttr.s() != null) {
                    uniqueSkiers.add(pkAttr.s());
                }
            }

            long numUniqueSkiers = uniqueSkiers.size();

            // Create the response JSON
            String responseJson = gson.toJson(Map.of(
                    "resort", resortID,
                    "numSkiers", numUniqueSkiers,
                    "response_code", 200
            ));

            // Cache the result in Redis with a TTL (e.g., 5 minutes)
            jedis.setex(redisKey, 1000, responseJson);

            return responseJson;

        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson(Map.of(
                    "response_code", 500,
                    "message", "Error retrieving number of unique skiers"
            ));
        }
    }



    // API2: GET/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}: get ski day vertical for a skier
    private String getDayVertical(Map<String, Object> request) {
        // Construct PK and SK prefix based on the request
        String pk = "SKIER#" + request.get("skierID");
        String skPrefix = "RESORT#" + request.get("resortID") + "#SEASON#" + request.get("seasonID") + "#DAY#" + request.get("dayID");

        try {
            // Query DynamoDB for all items matching PK and SK prefix
            QueryResponse result = dynamoDbClient.query(QueryRequest.builder()
                    .tableName(TABLE_NAME)
                    .keyConditionExpression(PartitionKey + " = :pk AND begins_with(" + SortKey + ", :skPrefix)")
                    .expressionAttributeValues(Map.of(
                            ":pk", AttributeValue.builder().s(pk).build(),
                            ":skPrefix", AttributeValue.builder().s(skPrefix).build()
                    ))
                    .build());

            // Check if no items are found
            if (result.items().isEmpty()) {
                // Return -1 and message for no data
                return gson.toJson(Map.of(
                        "total_vertical", -1,
                        "response_code", 404,
                        "message", "No data found"
                ));
            }

            // Iterate through the results and sum up the "vertical" field
            int totalVertical = result.items().stream()
                    .mapToInt(item -> Integer.parseInt(item.getOrDefault("vertical", AttributeValue.builder().n("0").build()).n()))
                    .sum();

            // Return total vertical in the response
            return gson.toJson(Map.of(
                    "response_code", 200,
                    "total_vertical", totalVertical
            ));

        } catch (Exception e) {
            e.printStackTrace();
            // Return error response with type and message
            return gson.toJson(Map.of(
                    "total_vertical", -1,
                    "response_code", 500,
                    "message", "Error retrieving day vertical"
            ));
        }
    }

    // API3: /skiers/{skierID}/vertical
    private String getResortVertical(Map<String, Object> request) {
        // Extract skierID and resortID from the request
        String skierID = (String) request.get("skierID");
        String pk = "SKIER#" + skierID;

        // Parse the resorts and seasons from the request
        List<String> resorts = (List<String>) request.get("resorts");
        List<String> seasons = (List<String>) request.get("seasons");

        String resortID = resorts.get(0); // Assume only one resortID is provided
        List<Map<String, Object>> seasonResults = new ArrayList<>();

        try {
            if (seasons == null || seasons.isEmpty()) {
                // If no seasons are specified, fetch all seasons for the specified resort
                String skPrefix = "RESORT#" + resortID;

                QueryResponse result = dynamoDbClient.query(QueryRequest.builder()
                        .tableName(TABLE_NAME)
                        .keyConditionExpression(PartitionKey + " = :pk AND begins_with(" + SortKey + ", :skPrefix)")
                        .expressionAttributeValues(Map.of(
                                ":pk", AttributeValue.builder().s(pk).build(),
                                ":skPrefix", AttributeValue.builder().s(skPrefix).build()
                        ))
                        .build());

                Map<String, Integer> seasonVerticals = new HashMap<>();
                for (Map<String, AttributeValue> item : result.items()) {
                    String sortKey = item.get(SortKey).s();
                    String seasonID = sortKey.split("#")[3]; // Extract the seasonID from the sort key
                    int vertical = Integer.parseInt(item.getOrDefault("vertical", AttributeValue.builder().n("0").build()).n());
                    seasonVerticals.put(seasonID, seasonVerticals.getOrDefault(seasonID, 0) + vertical);
                }

                for (Map.Entry<String, Integer> entry : seasonVerticals.entrySet()) {
                    seasonResults.add(Map.of(
                            "seasonID", entry.getKey(),
                            "totalVert", entry.getValue()
                    ));
                }
            } else {
                // Iterate over the list of seasons to filter
                for (String season : seasons) {
                    String skPrefix = "RESORT#" + resortID + "#SEASON#" + season;

                    QueryResponse result = dynamoDbClient.query(QueryRequest.builder()
                            .tableName(TABLE_NAME)
                            .keyConditionExpression(PartitionKey + " = :pk AND begins_with(" + SortKey + ", :skPrefix)")
                            .expressionAttributeValues(Map.of(
                                    ":pk", AttributeValue.builder().s(pk).build(),
                                    ":skPrefix", AttributeValue.builder().s(skPrefix).build()
                            ))
                            .build());

                    int totalVertical = result.items().stream()
                            .mapToInt(item -> Integer.parseInt(item.getOrDefault("vertical", AttributeValue.builder().n("0").build()).n()))
                            .sum();

                    seasonResults.add(Map.of(
                            "seasonID", season,
                            "totalVert", totalVertical
                    ));
                }
            }

            // Build the response
            return gson.toJson(Map.of(
                    "resorts", seasonResults,
                    "response_code", 200
            ));
        } catch (Exception e) {
            e.printStackTrace();
            // Return error response in case of exceptions
            return gson.toJson(Map.of(
                    "message", "Error retrieving resort vertical",
                    "response_code", 500
            ));
        }
    }


}


