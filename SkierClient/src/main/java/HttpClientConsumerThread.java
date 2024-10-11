import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import model.LiftRideUpdate;
import model.RequestMetrics;

public class HttpClientConsumerThread implements Runnable {
    private final BlockingQueue<LiftRideUpdate> eventQueue;
    private final ApiClient apiClient;
    private final SkiersApi apiInstance;
    private final int requestsPerThread;
    private final AtomicInteger failedRequests;
    private final AtomicInteger requestsSent;
    private final List<RequestMetrics> requestMetricsList; // Store metrics for each request

    public HttpClientConsumerThread(
            BlockingQueue<LiftRideUpdate> eventQueue,
            int requestsPerThread,
            AtomicInteger failedRequests,
            AtomicInteger requestsSent,
            List<RequestMetrics> requestMetricsList  // Pass the list to store metrics

    ) {
        this.eventQueue = eventQueue;
        this.apiClient = new ApiClient().setBasePath(Config.getBasePath());
        this.apiInstance = new SkiersApi(apiClient);
        this.requestsPerThread = requestsPerThread;
        this.failedRequests = failedRequests;
        this.requestsSent = requestsSent;
        this.requestMetricsList = requestMetricsList;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < requestsPerThread; i++) {
                if (requestsSent.get() >= HttpClientMultiThreaded.TOTAL_REQUESTS) {
                    break;
                }

                requestsSent.incrementAndGet();

                LiftRideUpdate event = eventQueue.take();
                long startTime = System.currentTimeMillis();
                boolean success = false;
                int responseCode = -1;
                int attempts = 0;

                while (!success && attempts <= HttpClientMultiThreaded.MAX_RETRIES) {
                    try {
                        apiInstance.writeNewLiftRide(
                                event.getLiftRideBody(),
                                event.getResortID(),
                                event.getSeasonID().toString(),
                                event.getDayID().toString(),
                                event.getSkierID()
                        );
                        success = true;
                        responseCode = 201;

                    } catch (ApiException e) {
                        int statusCode = e.getCode();
                        responseCode = statusCode;

                        // Handle 4XX and 5XX response codes with retries
                        if (statusCode >= 400 && statusCode < 600) {
                            System.err.println("Request failed with status code " + statusCode + ". Retrying (" + (attempts + 1) + "/5)...");
                            attempts++;
                        } else {
                            // Non-retrievable error, log and exit the retry loop
                            System.err.println("Request failed with non-retrievable status code " + statusCode + ". No retries.");
                            break;
                        }
                    }
                }

                long endTime = System.currentTimeMillis();  // Record end time
                long latency = endTime - startTime;

                // Store request metrics
                synchronized (requestMetricsList) {
                    requestMetricsList.add(new RequestMetrics(startTime, "POST", latency, responseCode));
                }

                if (!success) {
                    System.err.println("Failed to send request after " + HttpClientMultiThreaded.MAX_RETRIES + " attempts");
                    failedRequests.incrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
