import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import model.LiftRide;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@WebServlet(value = "/resorts/*")
public class ResortServlet extends HttpServlet {
    private static final int CHANNEL_POOL_SIZE = 200;

    // RabbitMQ constants
    private static final String GET_QUEUE_NAME = "skiersGetQueue";
    private static final String GET_RESPONSE_QUEUE_NAME = "skiersGetResponseQueue";
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

            // Initialize RMQChannelPool with default pool settings
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

        // Check we have a URL
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
                Map<String, Object> responseMap = gson.fromJson(response, Map.class);

                int responseCode = ((Double) responseMap.getOrDefault("response_code", 500)).intValue();
                resp.setStatus(responseCode);

                if (responseCode == 200) {
                    resp.getWriter().write(response);
                } else if (responseCode == 404) {
                    String errorMessage = (String) responseMap.get("message");
                    resp.getWriter().write(gson.toJson(Map.of(
                            "message", errorMessage
                    )));
                }

            } catch (Exception e) {
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                System.out.println(e.getMessage());
                resp.getWriter().write("{\"message\":\"Failed to process GET DAY VERTICAL request\"}");
            }

        }
        else {
            // Invalid URL length
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"message\":\"Invalid URL format\"}");
        }


    }

    private String sendGetRequestToQueue(String message) throws Exception {
        try (Channel channel = channelPool.borrowObject()) {
            String correlationId = java.util.UUID.randomUUID().toString();

            // Set up the message properties with reply-to and correlation ID
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(correlationId)
                    .replyTo(GET_RESPONSE_QUEUE_NAME) // Use shared reply queue
                    .build();

//            channel.queueDeclare(GET_QUEUE_NAME, true, false, false, null);
//            channel.queueDeclare(GET_RESPONSE_QUEUE_NAME, true, false, false, null);

            // Publish the GET request to the RabbitMQ queue
            channel.basicPublish("", GET_QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));

            // Wait for the response from the shared reply queue
            final String[] responseHolder = new String[1];
            channel.basicConsume(GET_RESPONSE_QUEUE_NAME, true, (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                    responseHolder[0] = new String(delivery.getBody(), StandardCharsets.UTF_8);
                }
            }, consumerTag -> {});

            // Poll for the response (timeout logic can be added if needed)
            while (responseHolder[0] == null) {
                Thread.sleep(10); // Sleep for a short duration to wait for the response
            }

            return responseHolder[0];
        }
    }

}
