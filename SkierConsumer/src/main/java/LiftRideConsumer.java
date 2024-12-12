import com.google.gson.Gson;
import com.rabbitmq.client.*;
import model.LiftRide;
import redis.clients.jedis.JedisPool;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class LiftRideConsumer {
    private static final String QUEUE_NAME = "skiersQueue";
    private static final int RMQ_CHANNEL_POOL_SIZE = 100;
    private static final int NUM_CONSUMER_THREADS = 800;
    private static final int BATCH_SIZE = 25;  // Number of messages per Redis batch
    private static final int MAX_RETRIES = 5;  // Retry attempts for Redis operations

    private static final String TABLE_NAME = "SkierNewTable";
    private static final String PartitionKey = "PK";
    private static final String SortKey = "SK";
    private static final String GSIPartitionKey = "GSI_PK";
    private static final String GSISortKey = "GSI_SK";

    private Connection connection;
    private RMQChannelPool channelPool;
    private JedisPool jedisPool; // Redis connection pool
    private DynamoDbClient dynamoDbClient;


    public static void main(String[] args) {
        new LiftRideConsumer().startConsuming();
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

            // Initialize RMQChannelPool with default pool settings
            channelPool = new RMQChannelPool(RMQ_CHANNEL_POOL_SIZE, new RMQChannelFactory(connection));

            // Initialize Redis connection pool using Singleton
//            jedisPool = RedisConnectionManager.getJedisPool();

            // Initialize DynamoDB client
            Region region = Region.US_WEST_2; // Change to the region specified in your AWS Learner Lab
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
        List<LiftRide> batch = new ArrayList<>();
        try {
            // Borrow a channel from the pool for each consumption cycle
            Channel channel = channelPool.borrowObject();
            channel.basicQos(100); // Prefetch 100 messages

            // Set up the message callback
            channel.basicConsume(QUEUE_NAME, false, (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                LiftRide liftRide = parseLiftRide(message);
                if (liftRide != null) {
                    batch.add(liftRide);

                    // If the batch size is reached, process it
                    if (batch.size() >= BATCH_SIZE) {
                        processBatch(batch);
                        batch.clear();
                    }
                }
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }, consumerTag -> {});

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Process any remaining messages in the batch
            if (!batch.isEmpty()) {
                processBatch(batch);
            }
        }
    }

    // ------------------------- DynamoDB persistence ---------------------------------
    private void processBatch(List<LiftRide> batch) {
        int retryCount = 0;
        int backoffTime = 100; // Initial backoff time in milliseconds

        while (retryCount <= MAX_RETRIES) {
            try {
                for (LiftRide liftRide : batch) {
                    // APIS
                    // PK:SKIER#{skierID}
                    // SK:RESORT#{resortID}#SEASON#{seasonID}#DAY#{dayID}
                    // GET/skiers/{skierID}/vertical: Get the total vertical for the skier for specified seasons at the specified resort (Params: resortID, seasonID(optional))
                    // GET/resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers: get number of unique skiers at resort/season/day
                    // GET/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}: get ski day vertical for a skier
                    // No need for GSI

                    // Primary Key (PK) and Sort Key (SK)
                    String pk = "SKIER#" + liftRide.getSkierID();
                    String sk = "RESORT#" + liftRide.getResortID() + "#SEASON#" + liftRide.getSeasonID() + "#DAY#" + liftRide.getDayID();

                    // GSI Keys
                    String gsiPk = "RESORT#" + liftRide.getResortID() + "#SEASON#" + liftRide.getSeasonID() + "#DAY#" + liftRide.getDayID();
//                    String gsiSk = "RESORT#" + liftRide.getResortID() + "#SEASON#" + liftRide.getSeasonID();

                    // Item Attributes
                    Map<String, AttributeValue> attributes = Map.of(
                            "time", AttributeValue.builder().s(String.valueOf(liftRide.getTime())).build(),
                            "liftID", AttributeValue.builder().s(String.valueOf(liftRide.getLiftID())).build(),
                            "vertical", AttributeValue.builder().n(String.valueOf(liftRide.getLiftID() * 10)).build(),
                            GSIPartitionKey, AttributeValue.builder().s(gsiPk).build()
//                            GSISortKey, AttributeValue.builder().s(gsiSk).build()
                    );

                    // Put item into DynamoDB
                    putItem(dynamoDbClient, pk, sk, attributes);
                }
                break;

            } catch (Exception e) {
                retryCount++;
                System.err.println("Batch processing failed. Retry " + retryCount + "/" + MAX_RETRIES);
                try {
                    Thread.sleep(backoffTime);
                    backoffTime *= 2; // Exponential backoff
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }


        if (retryCount > MAX_RETRIES) {
            System.err.println("Failed to process batch after " + MAX_RETRIES + " retries.");
        }
    }

    private static void putItem(DynamoDbClient dynamoDbClient, String pk, String sk, Map<String, AttributeValue> attributes) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PartitionKey, AttributeValue.builder().s(pk).build());
        item.put(SortKey, AttributeValue.builder().s(sk).build());
        item.putAll(attributes);

        PutItemRequest request = PutItemRequest.builder()
                .tableName(LiftRideConsumer.TABLE_NAME)
                .item(item)
                .build();

        try {
            dynamoDbClient.putItem(request);
            System.out.println("Successfully inserted item with PK: " + pk + " and SK: " + sk);
        } catch (Exception e) {
            System.err.println("Unable to insert item: " + e.getMessage());
        }
    }

// --------------- Redis persistence ---------------------------
//    private void processBatch(List<LiftRide> batch) {
//        int retryCount = 0;
//        int backoffTime = 100; // Initial backoff time in milliseconds
//
//        while (retryCount <= MAX_RETRIES) {
//            try (Jedis jedis = jedisPool.getResource()) {
//                Pipeline pipeline = jedis.pipelined();
//
//                for (LiftRide liftRide : batch) {
//                    // Redis key patterns
//                    String skierDaysKey = "skier:" + liftRide.getSkierID() + ":season:" + liftRide.getSeasonID() + ":days";
//                    String skierVerticalsKey = "skier:" + liftRide.getSkierID() + ":season:" + liftRide.getSeasonID() + ":verticals";
//                    String skierLiftsKey = "skier:" + liftRide.getSkierID() + ":season:" + liftRide.getSeasonID() + ":day:" + liftRide.getDayID() + ":lifts";
//                    String resortSkiersKey = "resort:" + liftRide.getResortID() + ":day:" + liftRide.getDayID() + ":skiers";
//
//                    // Add unique day to skier's record
//                    pipeline.sadd(skierDaysKey, String.valueOf(liftRide.getDayID()));
//
//                    // Increment vertical totals (LiftID * 10)
//                    int vertical = liftRide.getLiftID() * 10;
//                    pipeline.hincrBy(skierVerticalsKey, String.valueOf(liftRide.getDayID()), vertical);
//
//                    // Add lift ID to the skier's list of lifts ridden
//                    pipeline.rpush(skierLiftsKey, String.valueOf(liftRide.getLiftID()));
//
//                    // Add skier ID to the set of unique skiers for the resort on the day
//                    pipeline.sadd(resortSkiersKey, String.valueOf(liftRide.getSkierID()));
//                }
//
//                // Execute all commands in the pipeline
//                pipeline.sync();
//                break;
//
//            } catch (Exception e) {
//                retryCount++;
//                System.err.println("Batch processing failed. Retry " + retryCount + "/" + MAX_RETRIES);
//                try {
//                    Thread.sleep(backoffTime);
//                    backoffTime *= 2; // Exponential backoff
//                } catch (InterruptedException ex) {
//                    Thread.currentThread().interrupt();
//                    break;
//                }
//            }
//        }
//
//        if (retryCount > MAX_RETRIES) {
//            System.err.println("Failed to process batch after " + MAX_RETRIES + " retries.");
//        }
//    }

    private LiftRide parseLiftRide(String message) {
        try {
            return new Gson().fromJson(message, LiftRide.class); // Parse JSON to LiftRide object
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
