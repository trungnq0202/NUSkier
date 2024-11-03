package model;
import java.util.ArrayList;
import java.util.List;

public class LiftRideList {
    private final List<LiftRide> liftRides = new ArrayList<>();

    public synchronized void addLiftRide(LiftRide liftRide) {
        liftRides.add(liftRide);
    }

    public List<LiftRide> getLiftRides() {
        return liftRides;
    }
}