package io.pravega.benchmark.loadtest.handlers;

import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.benchmark.loadtest.utils.ArgumentsParser;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
public abstract class AbstractWorker implements Runnable {

    protected final int workerId;
    protected final AppConfig appConfig;
    protected final BlockingQueue<Stats> queue;
    protected final CountDownLatch latch;

    public AbstractWorker(final int workerId, final AppConfig appConfig, final BlockingQueue<Stats> queue, final CountDownLatch latch) throws Exception {
        this.workerId = workerId;
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

    protected Stats getStatsInfo(int eventSize, ArgumentsParser.RunMode runMode, String threadId) {
        Stats stats = new Stats();
        stats.setRunMode(runMode);
        stats.setAppId(appConfig.getAppName());
        stats.setThreadId(threadId);
        stats.setEventSize(eventSize);
        stats.setStartTime(Instant.now());
        return stats;
    }
}