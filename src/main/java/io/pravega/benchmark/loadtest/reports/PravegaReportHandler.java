package io.pravega.benchmark.loadtest.reports;

import io.pravega.benchmark.loadtest.utils.AppConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;

@Slf4j
public class PravegaReportHandler implements ReportHandler {

    private ClientFactory clientFactory = null;
    private EventStreamWriter<Stats> writer = null;
    private String routingKey;

    @Override
    public void open(AppConfig appConfig) {
        log.info("execution results will be published to {} ", appConfig.getReport());
        String scope = appConfig.getReport().getParams().stream().filter(p -> p.getKey().equals("scope")).findFirst().get().getValue();
        String stream = appConfig.getReport().getParams().stream().filter(p -> p.getKey().equals("stream")).findFirst().get().getValue();
        String controller = appConfig.getReport().getParams().stream().filter(p -> p.getKey().equals("controller")).findFirst().get().getValue();
        URI controllerUri = URI.create(controller);

        clientFactory = ClientFactory.withScope(scope,controllerUri);
        writer = clientFactory.createEventWriter(stream, new JavaSerializer<>(), EventWriterConfig.builder().build());
        routingKey = String.valueOf(System.currentTimeMillis());
    }

    @Override
    public void emit(Stats stats) {
        writer.writeEvent(routingKey, stats);
    }

    @Override
    public void close() {
        if (writer != null) {
            writer.close();
        }
        if (clientFactory != null) {
            clientFactory.close();
        }
    }
}
