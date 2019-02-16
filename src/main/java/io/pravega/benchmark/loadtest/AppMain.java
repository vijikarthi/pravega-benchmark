package io.pravega.benchmark.loadtest;

import io.pravega.benchmark.loadtest.handlers.AbstractHandler;
import io.pravega.benchmark.loadtest.handlers.HandlerFactory;
import io.pravega.benchmark.loadtest.reports.ReportFactory;
import io.pravega.benchmark.loadtest.reports.ReportHandler;
import io.pravega.benchmark.loadtest.reports.Reporter;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.benchmark.loadtest.utils.ArgumentsParser;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AppMain {
    public static void main(String ... args) {
        log.info("Application Main loaded");
        try {
            CountDownLatch latch = new CountDownLatch(2);

            AppConfig appConfig = ArgumentsParser.parseArgs(args);
            BlockingQueue<Stats> queue = new LinkedBlockingQueue<>();

            ReportHandler reportHandler = ReportFactory.getReportHandler(appConfig);
            reportHandler.open(appConfig);
            Reporter reporter = new Reporter(queue, reportHandler, appConfig, latch);

            ExecutorService executorService = Executors.newFixedThreadPool(1);
            executorService.submit(reporter);

            AbstractHandler task = HandlerFactory.getTaskHandler(appConfig, queue, latch);
            task.run();

            latch.await();
            reportHandler.close();
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error("Application failed", e);
        } finally {
            log.info("Application Main unloaded");
        }
    }
}