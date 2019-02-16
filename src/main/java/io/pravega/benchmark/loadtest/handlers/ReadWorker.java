package io.pravega.benchmark.loadtest.handlers;

import com.google.common.base.Preconditions;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ReadWorker extends AbstractWorker {

    private int workerId;
    private AtomicInteger readResponse;

    private ClientFactory clientFactory = null;
    private EventStreamReader<String> reader = null;

    private int eventSize;
    private int totalEvents;
    private URI controller;

    public ReadWorker(final int workerId, AtomicInteger readResponse, final AppConfig appConfig,
                      final BlockingQueue<Stats> queue, final CountDownLatch latch) throws Exception {
        super(appConfig, queue, latch);
        this.workerId = workerId;
        this.readResponse = readResponse;
        initialize();
    }

    protected void initialize() throws Exception {
        controller = URI.create(appConfig.getConnection().getController());
        eventSize = appConfig.getRead().getEventSize();
        totalEvents = appConfig.getWrite().getNoOfEvents();

        String defaultScope = appConfig.getConnection().getDefaultScope();
        String readerGroupScope = null;
        if (appConfig.getRead().getReaderGroup() != null) {
            readerGroupScope = appConfig.getRead().getReaderGroup().getScope();
        }
        Preconditions.checkArgument(defaultScope != null || readerGroupScope != null,
                "must supply either default scope or reader group scope");
        defaultScope = defaultScope != null ? defaultScope : readerGroupScope;

        ReaderGroupConfig.ReaderGroupConfigBuilder readerGroupConfigBuilder = ReaderGroupConfig.builder();

        for (AppConfig.StreamReadConfig streamReadConfig: appConfig.getRead().getStreams()) {
            String scope = streamReadConfig.getScope() != null ? streamReadConfig.getScope() : defaultScope;
            String stream = streamReadConfig.getStream();
            Preconditions.checkNotNull(stream, "stream cannot be null");
            String startStreamCut = streamReadConfig.getStartStreamCut();
            String endStreamCut = streamReadConfig.getEndStreamCut();

            Stream streamInfo = Stream.of(scope, stream);
            StreamCut start = null;
            StreamCut end = null;

            if (startStreamCut != null && startStreamCut.length() != 0) {
                start = StreamCut.from(startStreamCut);
            }

            if (endStreamCut != null && endStreamCut.length() != 0) {
                end = StreamCut.from(endStreamCut);
            }

            if (start != null && end != null) {
                readerGroupConfigBuilder.stream(streamInfo, start, end);
            } else if (start != null) {
                readerGroupConfigBuilder.stream(streamInfo, start);
            } else {
                readerGroupConfigBuilder.stream(streamInfo);
            }
        }

        String rgName = null;
        if (appConfig.getRead().getReaderGroup() != null) {
            rgName = appConfig.getRead().getReaderGroup().getName();
        }
        if (rgName == null || rgName.length() == 0) {
            rgName = UUID.randomUUID().toString().replace("-", "");
        }
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(defaultScope, controller);
        readerGroupManager.createReaderGroup(rgName, readerGroupConfigBuilder.build());

        clientFactory = ClientFactory.withScope(defaultScope, controller);
        reader = clientFactory.createReader(String.valueOf(workerId),
                rgName,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
    }

    @Override
    public void run() {
        log.info("reader thread {} is running now", Thread.currentThread().getName());
        try {
            while (readResponse.get() < totalEvents) {
                try {
                    Stats stats = new Stats();
                    stats.setStartTime(Instant.now());
                    stats.setEventSize(eventSize);
                    stats.setRunMode(appConfig.getRunMode());
                    EventRead<String> event = reader.readNextEvent(1000);
                    if (event.getEvent() != null) {
                        readResponse.incrementAndGet();
                        stats.setEndTime(Instant.now());
                        reportStats(stats);
                    }
                } catch (ReinitializationRequiredException e) {
                    log.warn("reader re-init required exception", e);
                }
            }
        } finally {
            latch.countDown();
            close();
            log.info("reader thread {} is exiting now. Total events read [{}]", Thread.currentThread().getName(), readResponse.get());
        }
    }

    private void close() {
        try {
            if (reader != null) {
                reader.close();
            }
            if (clientFactory != null) {
                clientFactory.close();
            }
        } catch (Exception e) {}
    }
}
