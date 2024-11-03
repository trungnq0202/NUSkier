package utils;

import model.RequestMetrics;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetricsUtils {
    public static void calculateAndDisplayMetrics(List<RequestMetrics> metrics, int numThreads, long totalTime, int totalRequests, int failedRequests) {
        double sumLatency = 0;
        long minLatency = Long.MAX_VALUE;
        long maxLatency = Long.MIN_VALUE;

        List<Long> latencies = new ArrayList<>();
        for (RequestMetrics metric : metrics) {
            long latency = metric.getLatency();
            latencies.add(latency);
            sumLatency += latency;
            if (latency < minLatency) minLatency = latency;
            if (latency > maxLatency) maxLatency = latency;
        }

        // Calculate statistics
        double meanLatency = sumLatency / latencies.size();
        Collections.sort(latencies);
        double medianLatency = latencies.get(latencies.size() / 2);
        double p99Latency = latencies.get((int) (latencies.size() * 0.99));
        double throughput = totalRequests / (totalTime / 1000.0);

        // Display the calculated metrics
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Total number of threads: " + numThreads);
        System.out.println("Total run time: " + totalTime + " ms");
        System.out.println("Total requests: " + totalRequests);
        System.out.println("Unsuccessful requests: " + failedRequests);
        System.out.println("Throughput: " + throughput + " requests/second");
        System.out.println("Mean response time: " + meanLatency + " ms");
        System.out.println("Median response time: " + medianLatency + " ms");
        System.out.println("99th percentile response time: " + p99Latency + " ms");
        System.out.println("Min response time: " + minLatency + " ms");
        System.out.println("Max response time: " + maxLatency + " ms");
        System.out.println("------------------------------------------------------------------------");
    }

    public static void writeMetricsToCSV(List<RequestMetrics> metrics, String fileName) throws IOException {
        FileWriter writer = new FileWriter(fileName);
        writer.write("StartTime,RequestType,Latency,ResponseCode\n");
        for (RequestMetrics metric : metrics) {
            writer.write(metric.getStartTime() + "," + metric.getRequestType() + "," + metric.getLatency() + "," + metric.getResponseCode() + "\n");
        }
        writer.close();
    }

}
