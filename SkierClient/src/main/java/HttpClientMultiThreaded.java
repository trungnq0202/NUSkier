import model.LiftRideUpdate;
import model.RequestMetrics;
import utils.MetricsUtils;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;
import java.util.*;

public class HttpClientMultiThreaded {
    public static final int TOTAL_REQUESTS = 200000;
    public static final int INITIAL_THREADS = 32;
    public static final int MAX_THREADS = 200;
    public static final int TOTAL_THREADS = MAX_THREADS + INITIAL_THREADS;
    public static final int INITIAL_REQUESTS_PER_THREAD = 1000;
    public static final int FINAL_REQUESTS_PER_THREAD = 1500;
    public static final int MAX_RETRIES = 5;

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        AtomicInteger failedRequests = new AtomicInteger(0);
        AtomicInteger requestsSent = new AtomicInteger(0);
        AtomicInteger activeThreads = new AtomicInteger(0);// Tracks active threads
        List<RequestMetrics> requestMetricsList = Collections.synchronizedList(new ArrayList<>());

        BlockingQueue<LiftRideUpdate> eventQueue = new LinkedBlockingQueue<>(50000);

        // Strictly limit thread creation with ThreadPoolExecutor
        ExecutorService executorService = new ThreadPoolExecutor(
                MAX_THREADS + 20,
                MAX_THREADS + 20,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(200),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        long startTime = System.currentTimeMillis();

        // Start producer thread to generate events
        Thread producerThread = new Thread(new HttpClientProducerThread(eventQueue, TOTAL_REQUESTS));
        producerThread.start();

        // Phase 1: Start with INITIAL_THREADS, each handling a batch of requests
        for (int i = 0; i < INITIAL_THREADS; i++) {
            submitTask(completionService, eventQueue, INITIAL_REQUESTS_PER_THREAD, failedRequests, requestsSent, activeThreads, requestMetricsList);
        }

        // Phase 2: Manage additional threads dynamically based on the requestsSent count
        while (requestsSent.get() < TOTAL_REQUESTS) {
            Future<Void> completedTask = completionService.take();  // Wait for any thread to finish
            completedTask.get();

            activeThreads.decrementAndGet();  // Decrease active thread count

            // Only submit new threads if we haven't exceeded the MAX_THREADS limit
            while (activeThreads.get() < TOTAL_THREADS + 20 && requestsSent.get() < TOTAL_REQUESTS) {
                submitTask(completionService, eventQueue, FINAL_REQUESTS_PER_THREAD, failedRequests, requestsSent, activeThreads, requestMetricsList);
            }

            Thread.sleep(5);
        }

        producerThread.join();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // Calculate and display metrics
        MetricsUtils.calculateAndDisplayMetrics(requestMetricsList, MAX_THREADS, totalTime, requestsSent.get(), failedRequests.get());

        // Write metrics to CSV
        MetricsUtils.writeMetricsToCSV(requestMetricsList, "request_metrics.csv");
    }

    private static void submitTask(
            CompletionService<Void> completionService,
            BlockingQueue<LiftRideUpdate> eventQueue,
            int requestsPerThread,
            AtomicInteger failedRequests,
            AtomicInteger requestsSent,
            AtomicInteger activeThreads,
            List<RequestMetrics> requestMetricsList // List to store metrics
    ) {
        activeThreads.incrementAndGet(); // Increase active thread count
        completionService.submit(() -> {
            HttpClientConsumerThread consumer = new HttpClientConsumerThread(
                    eventQueue, requestsPerThread, failedRequests, requestsSent, requestMetricsList
            );
            consumer.run();
            return null;
        });
    }
}
