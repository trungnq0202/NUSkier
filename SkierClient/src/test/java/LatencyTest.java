import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LatencyTest {
    ApiClient apiClient = new ApiClient().setBasePath(Config.getBasePath());
    private final SkiersApi api = new SkiersApi(apiClient);

    @Test
    public void singleThreadLatencyTest() throws Exception {
        int totalRequests = 10000;
        int successfulRequests = 0;
        int unsuccessfulRequests = 0;

        long startTime = System.currentTimeMillis();

        // Execute 10000 POST requests
        for (int i = 0; i < totalRequests; i++) {
            LiftRide body = new LiftRide();
            body.setLiftID(RandomGenerator.generateLiftID());
            body.setTime(RandomGenerator.generateTime());
            Integer resortID = RandomGenerator.generateResortID();
            String seasonID = RandomGenerator.generateSeasonID().toString();
            String dayID = RandomGenerator.generateDayID().toString();
            Integer skierID = RandomGenerator.generateSkierID();

            try {
                api.writeNewLiftRide(body, resortID, seasonID, dayID, skierID);
                successfulRequests++;
            } catch (ApiException e) {
                unsuccessfulRequests++;
            }
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;  // Total time in milliseconds

        // Calculate the throughput (requests per second)
        double throughput = (totalRequests / (totalTime / 1000.0));  // Convert totalTime to seconds

        System.out.println("Total requests: " + totalRequests);
        System.out.println("Successful requests: " + successfulRequests);
        System.out.println("Unsuccessful requests: " + unsuccessfulRequests);
        System.out.println("Total time: " + totalTime + " ms");
        System.out.println("Average latency: " + totalTime / totalRequests + "ms");
        System.out.println("Throughput: " + throughput + " requests/second");

        assertTrue("Total time should be greater than 0", totalTime > 0);
        assertEquals("There should be 0 unsuccessful requests", 0, unsuccessfulRequests);
    }
}
