package io.pravega.benchmark.loadtest.handlers;

import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
public abstract class AbstractWorker implements Runnable {

    protected final AppConfig appConfig;
    protected final BlockingQueue<Stats> queue;
    protected final CountDownLatch latch;

    public AbstractWorker(final AppConfig appConfig, final BlockingQueue<Stats> queue, final CountDownLatch latch) throws Exception {
        this.appConfig = appConfig;
        this.queue = queue;
        this.latch = latch;
    }

    protected void reportStats(Stats stats) {
        if (!queue.offer(stats)) {
            log.error("failed to add stats to the queue");
            throw new RuntimeException("failed to add stats to the queue");
        }
    }
}