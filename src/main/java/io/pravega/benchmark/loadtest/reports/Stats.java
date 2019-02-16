package io.pravega.benchmark.loadtest.reports;

import static io.pravega.benchmark.loadtest.utils.ArgumentsParser.RunMode;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.ToString;

import java.text.SimpleDateFormat;
import java.time.Instant;

@Data
@ToString
public class Stats {
    private Instant startTime;
    private Instant endTime;
    private int eventSize;
    private RunMode runMode;
    private boolean stop;

    private static SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy hh-mm-ss SSSz");

    public static String[] headers = {
            "runMode",
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
        return new String[] {
                runMode.name(),
                Integer.toString(eventSize),
                Instant.now().toString(),
                startTime.toString(),
                endTime.toString(),
                Long.toString(latency())
        };
    }
}
