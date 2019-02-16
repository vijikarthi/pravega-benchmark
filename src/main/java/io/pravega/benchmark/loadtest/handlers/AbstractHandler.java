package io.pravega.benchmark.loadtest.handlers;

import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Data
public abstract class AbstractHandler implements Closeable {

    protected final AppConfig appConfig;
    protected final BlockingQueue<Stats> queue;
    protected final CountDownLatch latch;

    public AbstractHandler(final AppConfig appConfig, final BlockingQueue<Stats> queue, final CountDownLatch latch) throws Exception {
        this.appConfig = appConfig;
        this.queue = queue;
        this.latch = latch;
    }

    public abstract void run() throws Exception;

    protected void stop() {
        Stats stats = new Stats();
        stats.setStop(true);
        queue.offer(stats);
    }
}