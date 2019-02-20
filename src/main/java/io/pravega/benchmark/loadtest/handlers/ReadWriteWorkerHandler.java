package io.pravega.benchmark.loadtest.handlers;

import com.google.common.util.concurrent.RateLimiter;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.client.admin.ReaderGroupManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ReadWriteWorkerHandler extends AbstractHandler {

    public ReadWriteWorkerHandler(AppConfig appConfig, BlockingQueue<Stats> queue, CountDownLatch latch) throws Exception {
        super(appConfig, queue, latch);
    }

    @Override
    public void run() throws Exception {

        int writeParallelism = appConfig.getWrite().getNoOfWriters();
        int readParallelism = appConfig.getRead().getNoOfReaders();

        CountDownLatch taskManagerLatch= new CountDownLatch(writeParallelism + readParallelism);
        ExecutorService taskManagerExecutorService = Executors.newFixedThreadPool(writeParallelism + readParallelism);

        List<Runnable> runnablesToManage = new ArrayList<>();

        log.info("initializing write workers");
        RateLimiter rateLimiter = null;
        if (appConfig.getWrite().getRequestRatePerSec() > 0) {
            rateLimiter = RateLimiter.create(appConfig.getWrite().getRequestRatePerSec(), 2000, TimeUnit.MILLISECONDS);
        }

        int totalEvents = appConfig.getWrite().getNoOfEvents();
        int totalEventsToGenerate = totalEvents / writeParallelism;
        int remaining = totalEvents - (totalEventsToGenerate * writeParallelism);

        for (int i=1; i <= writeParallelism; i++) {
            if (i == 1) {
                WriteWorker writeWorker = new WriteWorker(i, rateLimiter, appConfig, queue, taskManagerLatch, totalEventsToGenerate + remaining);
                runnablesToManage.add(writeWorker);
            } else {
                WriteWorker writeWorker = new WriteWorker(i, rateLimiter, appConfig, queue, taskManagerLatch, totalEventsToGenerate);
                runnablesToManage.add(writeWorker);
            }
        }

        log.info("initializing read workers");
        String rgName = null;
        if (appConfig.getRead().getReaderGroup() != null) {
            rgName = appConfig.getRead().getReaderGroup().getName();
        }
        if (rgName == null || rgName.length() == 0) {
            rgName = UUID.randomUUID().toString().replace("-", "");
        }

        AtomicInteger readerProgress = new AtomicInteger();
        for (int i=1; i <= readParallelism; i++) {
            ReadWorker readWorker = new ReadWorker(i, readerProgress, appConfig, queue, taskManagerLatch, rgName);
            runnablesToManage.add(readWorker);
        }


        try {
            log.info("executing write and read workers");
            CompletableFuture<?>[] futures = runnablesToManage.stream()
                    .map(task -> CompletableFuture.runAsync(task, taskManagerExecutorService))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(futures).join();

            taskManagerLatch.await();
        } finally {
            log.info("readwrite worker handler received notification from all writers and readers indicating they are done");
            taskManagerExecutorService.shutdown();
            taskManagerExecutorService.awaitTermination(20, TimeUnit.SECONDS);
            stop();
            latch.countDown();
            log.info("exiting readwrite worker handler");
        }
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

}