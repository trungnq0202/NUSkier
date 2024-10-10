import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.SkierVertical;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;


/**
 * API tests for SkiersApi
 */
@Ignore
public class SkiersApiTest {

    private final SkiersApi api = new SkiersApi();

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
        // TODO: add validations
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

        System.out.println("Lift ride recorded successfully.");
        // TODO: add validations
    }



    /**
     * get the total vertical for the skier for specified seasons at the specified resort
     *
     * get the total vertical for the skier the specified resort. If no season is specified, return all seasons
     *
     * @throws Exception
     *          if the Api call fails
     */
//    @Test
//    public void getSkierResortTotalsTest() throws Exception {
//        Integer skierID = null;
//        List<String> resort = null;
//        List<String> season = null;
//        SkierVertical response = api.getSkierResortTotals(skierID, resort, season);
//
//        // TODO: test validations
//        System.out.println(response);
//    }



}
