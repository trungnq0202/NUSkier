import model.LiftRideUpdate;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public class HttpClientProducerThread implements Runnable {
    private final BlockingQueue<LiftRideUpdate> eventQueue;
    private final int totalEvents;

    public HttpClientProducerThread(BlockingQueue<LiftRideUpdate> eventQueue, int totalEvents) {
        this.eventQueue = eventQueue;
        this.totalEvents = totalEvents;
    }

    @Override
    public void run() {
        for (int i = 0; i < totalEvents; i++) {
            LiftRideUpdate liftRideUpdate = generateRandomLiftRideUpdate();
            try {
                eventQueue.put(liftRideUpdate);  // Block if the queue is full
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private LiftRideUpdate generateRandomLiftRideUpdate() {
        // Replace with actual event generation logic
        return new LiftRideUpdate(
                RandomGenerator.generateSkierID(), RandomGenerator.generateResortID(),
                RandomGenerator.generateLiftID(), RandomGenerator.generateSeasonID(),
                RandomGenerator.generateDayID(), RandomGenerator.generateTime()
        );
    }
}
