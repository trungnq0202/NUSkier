import com.google.gson.Gson;
import com.rabbitmq.client.*;
import model.LiftRide;
import model.LiftRideList;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LiftRideConsumer {
    private static final String QUEUE_NAME = "skiersQueue";
    private static final int RMQ_CHANNEL_POOL_SIZE = 100;
    private static final int NUM_CONSUMERS_THREADS = 800;

    private Connection connection;
    private RMQChannelPool channelPool;

    private final ConcurrentHashMap<Integer, LiftRideList> skierDataMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        new LiftRideConsumer().startConsuming();
    }

    public void startConsuming() {
        // Set up ExecutorService for multiple consumers
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS_THREADS);

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

            for (int i = 0; i < NUM_CONSUMERS_THREADS; i++) {
                executor.submit(() -> consumeMessages());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void consumeMessages() {
        try {
            // Borrow a channel from the pool for each consumption cycle
            Channel channel = channelPool.borrowObject();
            channel.basicQos(1); // Process one message at a time

            // Set up the message callback
            channel.basicConsume(QUEUE_NAME, false, (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                processMessage(message);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }, consumerTag -> {});

            // Return the channel to the pool when finished
            channelPool.returnObject(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processMessage(String message) {
        // Parse the message JSON to retrieve skierID, liftID, etc.
        LiftRide liftRide = parseLiftRide(message);
        skierDataMap.computeIfAbsent(liftRide.getSkierID(), k -> new LiftRideList()).addLiftRide(liftRide);
    }

    private LiftRide parseLiftRide(String message) {
        try {
            return new Gson().fromJson(message, LiftRide.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
