package model;

public class LiftRide {
    private Integer skierID;
    private Integer resortID;
    private Integer seasonID;
    private Integer dayID;
    private Integer time;
    private Integer liftID;

    public LiftRide(Integer skierID, Integer resortID, Integer seasonID, Integer dayID, Integer time, Integer liftID) {
        this.skierID = skierID;
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.time = time;
        this.liftID = liftID;
    }

    public LiftRide(String[] urlParts, String jsonBody) {
        this.resortID = Integer.parseInt(urlParts[1]);
        this.seasonID = Integer.parseInt(urlParts[3]);
        this.dayID = Integer.parseInt(urlParts[5]);
        this.skierID = Integer.parseInt(urlParts[7]);

        this.time = Integer.parseInt(jsonBody.split("\"time\"")[1].split(":")[1].trim().split(",")[0]);
        this.liftID = Integer.parseInt(jsonBody.split("\"liftID\"")[1].split(":")[1].trim().split("}")[0]);
    }

    public Integer getSkierID() {
        return skierID;
    }

    public Integer getDayID() {
        return dayID;
    }

    public Integer getResortID() {
        return resortID;
    }

    public Integer getSeasonID() {
        return seasonID;
    }

    public Integer getTime() {
        return time;
    }

    public Integer getLiftID() {
        return liftID;
    }
}

