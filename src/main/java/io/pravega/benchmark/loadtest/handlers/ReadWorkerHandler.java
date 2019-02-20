package io.pravega.benchmark.loadtest.handlers;

import io.pravega.benchmark.loadtest.reports.Reporter;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
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
public class ReadWorkerHandler extends AbstractHandler {

    public ReadWorkerHandler(AppConfig appConfig, BlockingQueue<Stats> queue, CountDownLatch latch) throws Exception {
        super(appConfig, queue, latch);
    }

    @Override
    public void run() throws Exception {

        log.info("initializing read worker handler");
        int parallelism = appConfig.getRead().getNoOfReaders();
        CountDownLatch taskManagerLatch= new CountDownLatch(parallelism);
        ExecutorService taskManagerExecutorService = Executors.newFixedThreadPool(parallelism);
        List<Runnable> runnablesToManage = new ArrayList<>();
        AtomicInteger readerProgress = new AtomicInteger();

        String rgName = null;
        if (appConfig.getRead().getReaderGroup() != null) {
            rgName = appConfig.getRead().getReaderGroup().getName();
        }
        if (rgName == null || rgName.length() == 0) {
            rgName = UUID.randomUUID().toString().replace("-", "");
        }

        for (int i=1; i <= parallelism; i++) {
            ReadWorker readWorker = new ReadWorker(i, readerProgress, appConfig, queue, taskManagerLatch, rgName);
            runnablesToManage.add(readWorker);
        }

        try {
            log.info("running read workers");
            CompletableFuture<?>[] futures = runnablesToManage.stream()
                    .map(task -> CompletableFuture.runAsync(task, taskManagerExecutorService))
                    .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(futures).join();
            taskManagerLatch.await();
        } finally {
            log.info("read worker handler received notification from all readers indicating they are done");
            taskManagerExecutorService.shutdown();
            taskManagerExecutorService.awaitTermination(20, TimeUnit.SECONDS);
            stop();
            latch.countDown();
            log.info("exiting read worker handler");
        }
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

}
