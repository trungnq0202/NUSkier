import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import model.LiftRideUpdate;
import model.RequestMetrics;
import utils.MetricsUtils;

import java.util.concurrent.LinkedBlockingQueue;

public class ThreadBenchmark {
    public static final int TOTAL_REQUESTS = 200000;
    public static final int INITIAL_THREADS = 32;
    public static final int BATCH_REQUESTS = 1500;
    public static final int MAX_RETRIES = 5;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Array of different max thread counts to benchmark
//        int[] maxThreadCounts = {32, 48, 60, 72, 84, 96, 108, 120, 132};
        int[] maxThreadCounts = {32, 64, 128, 256, 512, 1024};

        for (int maxThreads : maxThreadCounts) {
            System.out.println("Benchmarking with maxThreads = " + maxThreads);
            runBenchmarkWithMaxThreads(maxThreads);
        }
    }

    private static void runBenchmarkWithMaxThreads(int maxThreads) throws InterruptedException, ExecutionException {
        AtomicInteger failedRequests = new AtomicInteger(0);
        AtomicInteger requestsSent = new AtomicInteger(0);
        AtomicInteger activeThreads = new AtomicInteger(0); // Tracks active threads
        List<RequestMetrics> requestMetricsList = Collections.synchronizedList(new ArrayList<>()); // Store metrics for each request

        BlockingQueue<LiftRideUpdate> eventQueue = new LinkedBlockingQueue<>(1000);

        // Strictly limit thread creation with ThreadPoolExecutor
        ExecutorService executorService = new ThreadPoolExecutor(
                maxThreads + 20,
                maxThreads + 20,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(500),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        long startTime = System.currentTimeMillis();

        // Start producer thread to generate events
        Thread producerThread = new Thread(new HttpClientProducerThread(eventQueue, TOTAL_REQUESTS));
        producerThread.start();

        // Phase 1: Start with INITIAL_THREADS, each handling a batch of requests
        for (int i = 0; i < INITIAL_THREADS; i++) {
            submitTask(completionService, eventQueue, 1000, failedRequests, requestsSent, activeThreads, requestMetricsList);
        }

        // Phase 2: Manage additional threads dynamically based on the requestsSent count
        while (requestsSent.get() < TOTAL_REQUESTS) {
            Future<Void> completedTask = completionService.take();  // Wait for any thread to finish
            completedTask.get();

            activeThreads.decrementAndGet();  // Decrease active thread count

            // Only submit new threads if we haven't exceeded the maxThreads limit
            while (activeThreads.get() < maxThreads + 20 && requestsSent.get() < TOTAL_REQUESTS) {
                submitTask(completionService, eventQueue, BATCH_REQUESTS, failedRequests, requestsSent, activeThreads, requestMetricsList);
            }
        }

        producerThread.join();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        MetricsUtils.calculateAndDisplayMetrics(requestMetricsList, maxThreads, totalTime, requestsSent.get(), failedRequests.get());
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
