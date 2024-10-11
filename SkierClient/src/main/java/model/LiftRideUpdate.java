package model;

import io.swagger.client.model.LiftRide;

public class LiftRideUpdate {
    private Integer skierID;
    private Integer resortID;
    private Integer seasonID;
    private Integer dayID;
    private LiftRide liftRideBody;

    public LiftRideUpdate(Integer skierID, Integer resortID, Integer liftID, Integer seasonID, Integer dayID, Integer time) {
        this.skierID = skierID;
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;

        this.liftRideBody = new LiftRide();
        this.liftRideBody.setLiftID(liftID);
        this.liftRideBody.setTime(time);
    }

    public LiftRide getLiftRideBody() {
        return liftRideBody;
    }

    public Integer getSkierID() {
        return skierID;
    }

    public Integer getResortID() {
        return resortID;
    }

    public Integer getSeasonID() {
        return seasonID;
    }

    public Integer getDayID() {
        return dayID;
    }
}
