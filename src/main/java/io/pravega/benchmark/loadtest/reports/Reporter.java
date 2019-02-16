package io.pravega.benchmark.loadtest.reports;

import io.pravega.benchmark.loadtest.utils.AppConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Data
public final class Reporter implements Runnable {

    private final BlockingQueue<Stats> queue;
    private final ReportHandler reportHandler;
    private final AppConfig appConfig;
    private final CountDownLatch latch;

    @Override
    public void run() {

        try {
            while (true) {
                try {
                    Stats stats = queue.take();
                    if (stats.isStop()) break;
                    reportHandler.emit(stats);
                } catch (Exception e) {
                    log.error("failed to read the data from queue", e);
                    break;
                }
            }
        } finally {
            latch.countDown();
            log.info("reporter thread exiting");
        }

    }
}
