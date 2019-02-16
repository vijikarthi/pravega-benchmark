package io.pravega.benchmark.loadtest.handlers;

import com.google.common.base.Preconditions;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class StreamCreateHandler extends AbstractHandler {

    public StreamCreateHandler(AppConfig appConfig, BlockingQueue<Stats> queue, CountDownLatch latch) throws Exception {
        super(appConfig, queue, latch);
    }

    @Override
    public void run() {

        try {
            final String controllerUri = appConfig.getConnection().getController();
            Preconditions.checkNotNull(controllerUri, "Missing controller URI information");

            ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerUri)).build();
            StreamManager streamManager = StreamManager.create(clientConfig);

            for (AppConfig.StreamInfo streamInfo : appConfig.getCreate().getStreams()) {
                final String scope = streamInfo.getScope() == null ? appConfig.getConnection().getDefaultScope() : streamInfo.getScope();
                final String stream = streamInfo.getStream();

                Preconditions.checkNotNull(scope, "Missing scope information");
                Preconditions.checkNotNull(stream, "Missing stream information");

                AppConfig.FixedScalingPolicy fixed = streamInfo.getPolicy().getFixed();
                AppConfig.DynamicScalingPolicy dynamic = streamInfo.getPolicy().getDynamic();

                ScalingPolicy scalingPolicy = null;
                Preconditions.checkArgument(fixed != null || dynamic == null, "scaling policy is required");
                if (fixed != null) {
                    scalingPolicy = ScalingPolicy.fixed(fixed.getNoOfSegments());
                } else {
                    Preconditions.checkArgument(dynamic.getTargetRate() > 0 || dynamic.getTargetKBps() > 0, "scaling policy is required");
                    if (dynamic.getTargetRate() > 0) {
                        scalingPolicy = ScalingPolicy.byDataRate(dynamic.getTargetKBps(), dynamic.getScaleFactor(), dynamic.getMinNumSegments());
                    } else {
                        scalingPolicy = ScalingPolicy.byEventRate(dynamic.getTargetRate(), dynamic.getScaleFactor(), dynamic.getMinNumSegments());
                    }
                }
                StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                        .scope(scope)
                        .streamName(stream)
                        .scalingPolicy(scalingPolicy)
                        .build();
                if (streamManager.createScope(scope)) {
                    log.info("created scope: {}", scope);
                } else {
                    log.info("scope: {} already exist", scope);
                }
                if (streamManager.createStream(scope, stream, streamConfiguration)) {
                    log.info("created stream: {}", stream);
                } else {
                    log.info("stream: {} already exist", stream);
                }
            }
        } finally {
            latch.countDown();
            stop();
            log.info("exiting create worker handler");
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

}
