import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@WebServlet(value = "/resorts/*")
public class ResortServlet extends HttpServlet {
    private static final int CHANNEL_POOL_SIZE = 200;

    // RabbitMQ constants
    private static final String GET_QUEUE_NAME = "skiersGetQueue";
    private static final String GET_NUM_UNIQUE_SKIERS_MESSAGE_KEY = "GET_NUM_UNIQUE_SKIERS";

    // Connections
    private final Gson gson = new Gson();
    private Connection connection;
    private RMQChannelPool channelPool;

    @Override
    public void init() {
        try {
            // Set up RabbitMQ connection
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(Config.getRMQHost());
            factory.setUsername(Config.getRMQUsername());
            factory.setPassword(Config.getRMQPassword());
            factory.setPort(Config.getRMQPort());

            connection = factory.newConnection();
            channelPool = new RMQChannelPool(CHANNEL_POOL_SIZE, new RMQChannelFactory(connection));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        try {
            channelPool.close();
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        String urlPath = req.getPathInfo();

        // Check if we have a URL
        if (urlPath == null || urlPath.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"message\":\"Missing parameters\"}");
            return;
        }

        String[] urlParts = urlPath.split("/");
        if (urlParts.length == 7) {
            // API 1: /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
            String resortID = urlParts[1];
            String seasonID = urlParts[3];
            String dayID = urlParts[5];

            try {
                String requestPayload = gson.toJson(Map.of(
                        "type", GET_NUM_UNIQUE_SKIERS_MESSAGE_KEY,
                        "resortID", resortID,
                        "seasonID", seasonID,
                        "dayID", dayID
                ));

                String response = sendGetRequestToQueue(requestPayload);

                if (response == null) {
                    // Timeout occurred
                    resp.setStatus(HttpServletResponse.SC_GATEWAY_TIMEOUT);
                    resp.getWriter().write("{\"message\":\"Request timed out\"}");
                    return;
                }

                Map<String, Object> responseMap = gson.fromJson(response, Map.class);
                int responseCode = ((Double) responseMap.getOrDefault("response_code", 500)).intValue();
                resp.setStatus(responseCode);

                if (responseCode == 200) {
                    resp.getWriter().write(response);
                } else {
                    String errorMessage = (String) responseMap.getOrDefault("message", "Unknown error");
                    resp.getWriter().write(gson.toJson(Map.of(
                            "message", errorMessage
                    )));
                }
            } catch (Exception e) {
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                System.err.println("Error processing GET request: " + e.getMessage());
                resp.getWriter().write("{\"message\":\"Failed to process GET UNIQUE SKIERS request\"}");
            }
        } else {
            // Invalid URL length
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"message\":\"Invalid URL format\"}");
        }
    }

    private String sendGetRequestToQueue(String message) throws Exception {
        Channel channel = null;
        final long TIMEOUT_MS = 15000; // Timeout duration in milliseconds
        final String correlationId = java.util.UUID.randomUUID().toString();

        try {
            channel = channelPool.borrowObject(); // Borrow channel from pool

            // Create a unique, temporary reply queue for this request
            String replyQueueName = channel.queueDeclare("", false, true, true, null).getQueue();

            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(correlationId)
                    .replyTo(replyQueueName)
                    .build();

            // Publish request to the request queue
            channel.basicPublish("", GET_QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("Published request with correlationId: " + correlationId);

            final String[] responseHolder = new String[1];
            CountDownLatch latch = new CountDownLatch(1);

            // Consume from the temporary reply queue
            String consumerTag = channel.basicConsume(replyQueueName, true, (ct, delivery) -> {
                if (correlationId.equals(delivery.getProperties().getCorrelationId())) {
                    responseHolder[0] = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    latch.countDown();
                }
            }, ct -> {
                System.err.println("Consumer canceled: " + ct);
            });

            // Wait for response or timeout
            if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                // Timeout
                channel.basicCancel(consumerTag);
                System.err.println("Timeout waiting for response with correlationId: " + correlationId);
                return null;
            }

            channel.basicCancel(consumerTag);
            return responseHolder[0];
        } catch (Exception e) {
            System.err.println("Error in sendGetRequestToQueue: " + e.getMessage());
            throw e;
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
        }
    }
}
