package io.pravega.benchmark.loadtest.handlers;

import com.google.common.util.concurrent.RateLimiter;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WriteWorkerHandler extends AbstractHandler {

    public WriteWorkerHandler(AppConfig appConfig, BlockingQueue<Stats> queue, CountDownLatch latch) throws Exception {
        super(appConfig, queue, latch);
    }

    @Override
    public void run() throws Exception {

        log.info("initializing write worker handler");
        int parallelism = appConfig.getWrite().getNoOfWriters();
        CountDownLatch taskManagerLatch= new CountDownLatch(parallelism);
        ExecutorService taskManagerExecutorService = Executors.newFixedThreadPool(parallelism);
        List<Runnable> runnablesToManage = new ArrayList<>();
        RateLimiter rateLimiter = null;
        if (appConfig.getWrite().getRequestRatePerSec() > 0) {
            rateLimiter = RateLimiter.create(appConfig.getWrite().getRequestRatePerSec(), 2000, TimeUnit.MILLISECONDS);
        }

        int totalEvents = appConfig.getWrite().getNoOfEvents();
        int totalEventsToGenerate = totalEvents / parallelism;
        int remaining = totalEvents - (totalEventsToGenerate * parallelism);

        for (int i=1; i <= parallelism; i++) {
            if (i == 1) {
                WriteWorker writeWorker = new WriteWorker(i, rateLimiter, appConfig, queue, taskManagerLatch, totalEventsToGenerate + remaining);
                runnablesToManage.add(writeWorker);
            } else {
                WriteWorker writeWorker = new WriteWorker(i, rateLimiter, appConfig, queue, taskManagerLatch, totalEventsToGenerate);
                runnablesToManage.add(writeWorker);
            }
        }

        try {
            log.info("running write workers");

            CompletableFuture<?>[] futures = runnablesToManage.stream()
                    .map(task -> CompletableFuture.runAsync(task, taskManagerExecutorService))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(futures).join();

            taskManagerLatch.await();
        } finally {
            taskManagerExecutorService.shutdown();
            taskManagerExecutorService.awaitTermination(20, TimeUnit.SECONDS);
            stop();
            latch.countDown();
            log.info("exiting write worker handler");
        }
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

}