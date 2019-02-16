package io.pravega.benchmark.loadtest.handlers;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WriteWorker extends AbstractWorker {

    private int workerId;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private AtomicInteger ongoingRequest = new AtomicInteger();

    private ClientFactory clientFactory = null;
    private EventStreamWriter<String> writer = null;

    private final RateLimiter rateLimiter;

    private String scope;
    private String stream;
    private int eventSize;
    private int totalEvents;
    private int parallelism;
    private boolean useStaticData;
    private boolean useRandomKey;
    private int totalEventsToGenerate;
    private URI controller;
    private String data;
    private String routingKey;


    public WriteWorker(final int workerId, final RateLimiter rateLimiter, final AppConfig appConfig,
                       final BlockingQueue<Stats> queue, final CountDownLatch latch) throws Exception {
        super(appConfig, queue, latch);
        this.workerId = workerId;
        this.routingKey = String.valueOf(workerId);
        this.rateLimiter = rateLimiter;
        initialize();
    }

    protected void initialize() throws Exception {

        Preconditions.checkArgument(appConfig.getWrite().getScope() != null ||
                appConfig.getConnection().getDefaultScope() != null, "Missing scope information");
        Preconditions.checkNotNull(appConfig.getWrite().getStream(), "Missing stream information");
        Preconditions.checkNotNull(appConfig.getConnection().getController(), "Missing controller URI information");

        scope = appConfig.getWrite().getScope() == null ?
                appConfig.getConnection().getDefaultScope() : appConfig.getWrite().getScope();
        stream = appConfig.getWrite().getStream();
        controller = URI.create(appConfig.getConnection().getController());

        eventSize = appConfig.getWrite().getEventSize();
        totalEvents = appConfig.getWrite().getNoOfEvents();
        useRandomKey = appConfig.getWrite().isUseRandomKey();
        useStaticData = appConfig.getWrite().isUseStaticData();
        parallelism = appConfig.getWrite().getNoOfWriters();
        totalEventsToGenerate = totalEvents / parallelism;

        clientFactory = ClientFactory.withScope(scope,controller);
        writer = clientFactory.createEventWriter(stream, new JavaSerializer<>(), EventWriterConfig.builder().build());

        if (useStaticData) {
            data = RandomStringUtils.randomAlphabetic(eventSize);
        }
    }

    @Override
    public void run() {
        log.info("writer thread {} is running now", Thread.currentThread().getName());
        int currentOffset = 1;
        try {
            while (currentOffset <= totalEventsToGenerate) {

                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }

                CompletableFuture<Void> future;

                Stats stats = new Stats();
                stats.setStartTime(Instant.now());
                stats.setEventSize(eventSize);
                stats.setRunMode(appConfig.getRunMode());

                if (useRandomKey) {
                    future = writer.writeEvent(getData());
                } else {
                    future = writer.writeEvent(routingKey, getData());
                }

                ongoingRequest.incrementAndGet();
                future.whenCompleteAsync(
                        (result, e) -> {
                            if (e != null) {
                                log.error("unable to write the event", e);
                            } else {
                                stats.setEndTime(Instant.now());
                                reportStats(stats);
                            }
                            ongoingRequest.decrementAndGet();
                        }, executorService
                );
                currentOffset++;
            }
            while (ongoingRequest.get() > 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) { }
            }
        } finally {
            latch.countDown();
            close();
            log.info("writer thread {} is exiting now. Wrote [{}] records", Thread.currentThread().getName(), currentOffset);
        }
    }

    private void close() {

        try {
            if (writer != null) {
                writer.close();
            }
            if (clientFactory != null) {
                clientFactory.close();
            }
            if (executorService != null) {
                executorService.shutdown();
            }
        } catch (Exception e) {}
    }

    private String getData() {
        if (!useStaticData) {
            return RandomStringUtils.randomAlphabetic(eventSize);
        }
        return data;
    }
}
