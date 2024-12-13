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
    private static final int REDIS_TTL = 1000;

    // DynamoDB constants
    private static final String TABLE_NAME = "SkierTable";
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
        String redisKey = "uniqueSkiers:" + gsiPK;

        try (Jedis jedis = jedisPool.getResource()) {
            String cachedResult = jedis.get(redisKey);
            if (cachedResult != null) {
                return cachedResult;
            }

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

            String responseJson = gson.toJson(Map.of(
                    "resort", resortID,
                    "numSkiers", numUniqueSkiers,
                    "response_code", 200
            ));

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
        String pk = "SKIER#" + request.get("skierID");
        String skPrefix = "RESORT#" + request.get("resortID") + "#SEASON#" + request.get("seasonID") + "#DAY#" + request.get("dayID");
        String redisKey = "dayVertical:" + pk + ":" + skPrefix;

        try (Jedis jedis = jedisPool.getResource()) {
            String cachedResult = jedis.get(redisKey);
            if (cachedResult != null) {
                return cachedResult;
            }

            QueryResponse result = dynamoDbClient.query(QueryRequest.builder()
                    .tableName(TABLE_NAME)
                    .keyConditionExpression(PartitionKey + " = :pk AND begins_with(" + SortKey + ", :skPrefix)")
                    .expressionAttributeValues(Map.of(
                            ":pk", AttributeValue.builder().s(pk).build(),
                            ":skPrefix", AttributeValue.builder().s(skPrefix).build()
                    ))
                    .build());

            if (result.items().isEmpty()) {
                String responseJson = gson.toJson(Map.of(
                        "total_vertical", -1,
                        "response_code", 200,
                        "message", "No data found"
                ));
                jedis.setex(redisKey, REDIS_TTL, responseJson); // Cache no-data response
                return responseJson;
            }

            int totalVertical = result.items().stream()
                    .mapToInt(item -> Integer.parseInt(item.getOrDefault("vertical", AttributeValue.builder().n("0").build()).n()))
                    .sum();

            String responseJson = gson.toJson(Map.of(
                    "response_code", 200,
                    "total_vertical", totalVertical
            ));

            jedis.setex(redisKey, REDIS_TTL, responseJson);

            return responseJson;

        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson(Map.of(
                    "total_vertical", -1,
                    "response_code", 500,
                    "message", "Error retrieving day vertical"
            ));
        }
    }


    // API3: /skiers/{skierID}/vertical
    private String getResortVertical(Map<String, Object> request) {
        String skierID = (String) request.get("skierID");
        String pk = "SKIER#" + skierID;
        List<String> resorts = (List<String>) request.get("resorts");
        List<String> seasons = (List<String>) request.get("seasons");

        String resortID = resorts.get(0);
        String redisKey = "resortVertical:" + skierID + ":" + resortID + (seasons != null ? ":" + String.join(",", seasons) : "");

        try (Jedis jedis = jedisPool.getResource()) {
            String cachedResult = jedis.get(redisKey);
            if (cachedResult != null) {
                return cachedResult;
            }

            List<Map<String, Object>> seasonResults = new ArrayList<>();
            if (seasons == null || seasons.isEmpty()) {
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

            String responseJson = gson.toJson(Map.of(
                    "resorts", seasonResults,
                    "response_code", 200
            ));

            jedis.setex(redisKey, REDIS_TTL, responseJson);
            return responseJson;

        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson(Map.of(
                    "message", "Error retrieving resort vertical",
                    "response_code", 500
            ));
        }
    }


}


