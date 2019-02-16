package io.pravega.benchmark.loadtest.handlers;

import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.benchmark.loadtest.utils.ArgumentsParser;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import static io.pravega.benchmark.loadtest.utils.ArgumentsParser.supportedRunMode;

public class HandlerFactory {

    public static AbstractHandler getTaskHandler(AppConfig appConfig, BlockingQueue<Stats> queue, CountDownLatch latch) throws Exception {
        String mode = appConfig.getRunMode().name();
        if (mode.equals(ArgumentsParser.RunMode.create.name())) {
            return new StreamCreateHandler(appConfig, queue, latch);
        } else if (mode.equals(ArgumentsParser.RunMode.read.name())) {
            return new ReadWorkerHandler(appConfig, queue, latch);
        } else if (mode.equals(ArgumentsParser.RunMode.write.name())) {
            return new WriteWorkerHandler(appConfig, queue, latch);
        }
        String message = "Invalid task type [" + appConfig.getRunMode().name() + "] requested. Supported values are "+ supportedRunMode();
        throw new UnsupportedOperationException(message);
    }

}
