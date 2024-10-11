import io.swagger.client.ApiClient;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * API tests for SkiersApi
 */
@Ignore
public class SkiersApiTest {
    ApiClient apiClient = new ApiClient().setBasePath(Config.getBasePath());
    private final SkiersApi api = new SkiersApi(apiClient);

    /**
     * get ski day vertical for a skier
     *
     * get the total vertical for the skier for the specified ski day
     *
     * @throws Exception
     *          if the Api call fails
     */
    @Test
    public void getSkierDayVerticalTest() throws Exception {
        Integer resortID = 1;
        String seasonID = "2023";
        String dayID = "1";
        Integer skierID = 123;
        Integer response = api.getSkierDayVertical(resortID, seasonID, dayID, skierID);

        System.out.println(response);  // Print or validate response

        // Check for dummy data
        assertEquals("The vertical should be 12345", Integer.valueOf(12345), response);
        System.out.println("getSkierDayVerticalTest() passed!");
    }

    /**
     * write a new lift ride for the skier
     *
     * Stores new lift ride details in the data store
     *
     * @throws Exception
     *          if the Api call fails
     */
    @Test
    public void writeNewLiftRideTest() throws Exception {
        LiftRide body = new LiftRide();
        body.setLiftID(5);  // Example lift ID
        body.setTime(10);   // Example time

        Integer resortID = 1;  // Replace with actual resort ID
        String seasonID = "2023";  // Replace with actual season ID
        String dayID = "1";  // Replace with actual day ID
        Integer skierID = 123;  // Replace with actual skier ID
        api.writeNewLiftRide(body, resortID, seasonID, dayID, skierID);

        System.out.println("writeNewLiftRideTest() passed!");
    }
}
