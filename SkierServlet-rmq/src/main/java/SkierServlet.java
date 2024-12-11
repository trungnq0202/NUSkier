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

@WebServlet(value = "/skiers/*")
public class SkierServlet extends HttpServlet {
    private static final int CHANNEL_POOL_SIZE = 200;

    // RabbitMQ constants
    private static final String POST_QUEUE_NAME = "skiersQueue";
    private static final String GET_QUEUE_NAME = "skiersGetQueue";
    private static final String GET_RESPONSE_QUEUE_NAME = "skiersGetResponseQueue";
    private static final String GET_TOTAL_DAY_VERTICAL_MESSAGE_KEY = "GET_DAY_VERTICAL";
    private static final String GET_TOTAL_RESORT_VERTICAL_MESSAGE_KEY = "GET_RESORT_VERTICAL";

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

        // API 2: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        if (urlParts.length == 8) {
            if (!isGetUrlValid(urlParts)) {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getWriter().write("{\"message\":\"Invalid URL format\"}");
                return;
            }

            String resortID = urlParts[1];
            String seasonID = urlParts[3];
            String dayID = urlParts[5];
            String skierID = urlParts[7];

            try {
                String requestPayload = gson.toJson(Map.of(
                        "type", GET_TOTAL_DAY_VERTICAL_MESSAGE_KEY,
                        "resortID", resortID,
                        "seasonID", seasonID,
                        "dayID", dayID,
                        "skierID", skierID
                ));

                String response = sendGetRequestToQueue(requestPayload);
                Map<String, Object> responseMap = gson.fromJson(response, Map.class);

                int responseCode = ((Double) responseMap.getOrDefault("response_code", 500)).intValue();
                resp.setStatus(responseCode);

                if (responseCode == 200) {
                    int totalVertical = ((Double) responseMap.get("total_vertical")).intValue();
                    resp.getWriter().write(String.valueOf(totalVertical));
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

        // API 3: /skiers/{skierID}/vertical
        else if (urlParts.length == 3) {
            String skierID = urlParts[1];

            // Parse optional query parameters
            String[] resorts = req.getParameterValues("resort");
            String[] seasons = req.getParameterValues("season");

            try {
                // Create request payload for the GET consumer
                String requestPayload = gson.toJson(Map.of(
                        "type", GET_TOTAL_RESORT_VERTICAL_MESSAGE_KEY,
                        "skierID", skierID,
                        "resorts", resorts != null ? List.of(resorts) : List.of(),
                        "seasons", seasons != null ? List.of(seasons) : List.of()
                ));

                String response = sendGetRequestToQueue(requestPayload);
                Map<String, Object> responseMap = gson.fromJson(response, Map.class);

                int responseCode = ((Double) responseMap.getOrDefault("response_code", 500)).intValue();
                resp.setStatus(responseCode);
                if (responseCode == 200) {
                    // TODO: Parse response
                    resp.getWriter().write(response);
                } else {
                    String errorMessage = (String) responseMap.get("message");
                    resp.getWriter().write(gson.toJson(Map.of(
                            "message", errorMessage
                    )));
                }
            } catch (Exception e) {
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                System.out.println(e.getMessage());
                resp.getWriter().write("{\"message\":\"Failed to process GET RESORT VERTICAL request\"}");
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

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");

        String urlPath = req.getPathInfo();

        // Check we have a URL
        if (urlPath == null || urlPath.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
            resp.getWriter().write("{\"message\":\"Missing parameters\"}");
            return;
        }

        String[] urlParts = urlPath.split("/");

        // Validate the URL
        if (!isPostUrlValid(urlParts)) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"message\":\"Invalid URL format\"}");
            return;
        }

        // Validate the JSON body
        String jsonBody = req.getReader().lines().reduce("", String::concat);
        if (!isPostBodyValid(jsonBody)) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"message\":\"Invalid body parameters\"}");
            return;
        }

        // If everything is valid
        try {
            LiftRide liftRide = new LiftRide(urlParts, jsonBody);
            sendToQueue(gson.toJson(liftRide));
            resp.setStatus(HttpServletResponse.SC_CREATED);
        } catch (Exception e) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().write("{\"message\":\"Failed to send data to the queue\"}");
//            e.printStackTrace();
        }
    }

    private boolean isGetUrlValid(String[] urlParts) {
        return validateGetPostURL(urlParts);
    }

    // Validate the URL for POST /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    private boolean isPostUrlValid(String[] urlParts) {
        return validateGetPostURL(urlParts);
    }

    private boolean validateGetPostURL(String[] urlParts) {
        // Validate the URL length
        if (urlParts.length != 8) {
            return false;
        }

        String resortID = urlParts[1];
        String seasonPath = urlParts[2];
        String seasonID = urlParts[3];
        String dayPath = urlParts[4];
        String dayID = urlParts[5];
        String skierPath = urlParts[6];
        String skierID = urlParts[7];

        // Validate that resortID and skierID are integers
        if (!resortID.matches("\\d+")) return false;           // Resort ID must be numeric (integer)
        if (!skierID.matches("\\d+")) return false;            // Skier ID must be numeric (integer)

        // Validate the static path strings
        if (!seasonPath.equals("seasons")) return false;       // Expected "seasons" path
        if (!dayPath.equals("days")) return false;             // Expected "days" path
        if (!skierPath.equals("skiers")) return false;         // Expected "skiers" path

        // Validate seasonID is a 4-digit year
        if (!seasonID.matches("\\d{4}")) return false;         // Season ID must be a valid 4-digit year

        // Validate dayID is between 1 and 366
        try {
            int dayIDInt = Integer.parseInt(dayID);
            if (dayIDInt < 1 || dayIDInt > 366) return false;  // Day ID must be between 1 and 366
        } catch (NumberFormatException e) {
            return false;
        }

        return true;
    }

    // Validate the JSON body for POST request
    private boolean isPostBodyValid(String jsonBody) {
        // Basic validation to check for required fields in JSON body: "time" and "liftID"
        if (!jsonBody.contains("\"time\"") || !jsonBody.contains("\"liftID\"")) {
            return false;
        }

        try {
            // Assuming a simple string match here for demo purposes, but this can be improved
            int time = Integer.parseInt(jsonBody.split("\"time\"")[1].split(":")[1].trim().split(",")[0]);
            int liftID = Integer.parseInt(jsonBody.split("\"liftID\"")[1].split(":")[1].trim().split("}")[0]);

            // Validate time and liftID (simple validation rules could be added here)
            if (time < 0 || time > 360) return false;  // Assuming time is in minutes or some similar range
            if (liftID < 1) return false;              // Assuming liftID should be a positive integer
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private void sendToQueue(String message) {
        try {
            // Borrow a channel from the pool
            Channel channel = channelPool.borrowObject();
            try {
//                channel.queueDeclare(POST_QUEUE_NAME, true, false, false, null);
                channel.basicPublish("", POST_QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            } finally {
                channelPool.returnObject(channel); // Return channel to the pool
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
