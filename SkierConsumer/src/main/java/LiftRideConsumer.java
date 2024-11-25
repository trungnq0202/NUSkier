import com.google.gson.Gson;
import com.rabbitmq.client.*;
import model.LiftRide;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LiftRideConsumer {
    private static final String QUEUE_NAME = "skiersQueue";
    private static final int RMQ_CHANNEL_POOL_SIZE = 100;
    private static final int NUM_CONSUMER_THREADS = 800;
    private static final int BATCH_SIZE = 10;  // Number of messages per Redis batch
    private static final int MAX_RETRIES = 5;  // Retry attempts for Redis operations

    private Connection connection;
    private RMQChannelPool channelPool;
    private JedisPool jedisPool; // Redis connection pool

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
            jedisPool = RedisConnectionManager.getJedisPool();

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

    private void processBatch(List<LiftRide> batch) {
        int retryCount = 0;
        int backoffTime = 100; // Initial backoff time in milliseconds

        while (retryCount <= MAX_RETRIES) {
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipeline = jedis.pipelined();

                for (LiftRide liftRide : batch) {
                    // Redis key patterns
                    String skierDaysKey = "skier:" + liftRide.getSkierID() + ":season:" + liftRide.getSeasonID() + ":days";
                    String skierVerticalsKey = "skier:" + liftRide.getSkierID() + ":season:" + liftRide.getSeasonID() + ":verticals";
                    String skierLiftsKey = "skier:" + liftRide.getSkierID() + ":season:" + liftRide.getSeasonID() + ":day:" + liftRide.getDayID() + ":lifts";
                    String resortSkiersKey = "resort:" + liftRide.getResortID() + ":day:" + liftRide.getDayID() + ":skiers";

                    // Add unique day to skier's record
                    pipeline.sadd(skierDaysKey, String.valueOf(liftRide.getDayID()));

                    // Increment vertical totals (LiftID * 10)
                    int vertical = liftRide.getLiftID() * 10;
                    pipeline.hincrBy(skierVerticalsKey, String.valueOf(liftRide.getDayID()), vertical);

                    // Add lift ID to the skier's list of lifts ridden
                    pipeline.rpush(skierLiftsKey, String.valueOf(liftRide.getLiftID()));

                    // Add skier ID to the set of unique skiers for the resort on the day
                    pipeline.sadd(resortSkiersKey, String.valueOf(liftRide.getSkierID()));
                }

                // Execute all commands in the pipeline
                pipeline.sync();
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

    private LiftRide parseLiftRide(String message) {
        try {
            return new Gson().fromJson(message, LiftRide.class); // Parse JSON to LiftRide object
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
