package io.pravega.benchmark.loadtest.reports;

import static io.pravega.benchmark.loadtest.utils.ArgumentsParser.RunMode;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Instant;

@Data
@ToString
public class Stats implements Serializable {


    private boolean stop;

    private RunMode runMode;
    private String appId;
    private String threadId;
    private String eventKey;
    private int eventSize;
    private Instant eventTime;
    private Instant startTime;
    private Instant endTime;
    private long latency;

    private static SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy hh-mm-ss SSSz");

    public static String[] headers = {
            "runMode",
            "appId",
            "threadId",
            "eventKey",
            "eventSize",
            "eventTime",
            "startTime",
            "endTime",
            "latencyInMilliSec"
    };

    public long latency() {
        Preconditions.checkNotNull(startTime, "startTime cannot be null");
        Preconditions.checkNotNull(endTime, "endTime cannot be null");
        return endTime.toEpochMilli() - startTime.toEpochMilli();
    }

    public String[] data() {
        this.eventTime = Instant.now();
        this.latency = latency();
        return new String[] {
                runMode.name(),
                appId,
                threadId,
                eventKey,
                Integer.toString(eventSize),
                eventTime.toString(),
                startTime.toString(),
                endTime.toString(),
                Long.toString(latency)
        };
    }
}
