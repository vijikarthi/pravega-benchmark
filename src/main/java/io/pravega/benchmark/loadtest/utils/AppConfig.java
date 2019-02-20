package io.pravega.benchmark.loadtest.utils;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.pravega.benchmark.loadtest.handlers.WriteWorker.MAX_PAD_LENGTH;
import static io.pravega.benchmark.loadtest.utils.ArgumentsParser.RunMode;

@Data
@ToString
public final class AppConfig {

    private String appName;
    private ConnectionConfig connection;
    private CreateConfig create;
    private WriteConfig write;
    private ReadConfig read;
    private Report report;
    private RunMode runMode;

    void validate() {
        Preconditions.checkNotNull(connection, "connection cannot be null");
        connection.validate();

        if (runMode == RunMode.create) {
            Preconditions.checkNotNull(create, "create cannot be null");
            create.connection = connection;
            create.validate();
        }
        else if (runMode == RunMode.write) {
            Preconditions.checkNotNull(write, "write cannot be null");
            write.connection = connection;
            write.validate();
        }
        else if (runMode == RunMode.read) {
            Preconditions.checkNotNull(read, "read cannot be null");
            read.connection = connection;
            read.validate();
        } else if (runMode == RunMode.readwrite) {
            Preconditions.checkNotNull(write, "write cannot be null");
            write.connection = connection;
            write.validate();
            Preconditions.checkNotNull(read, "read cannot be null");
            read.connection = connection;
            read.validate();
        }

        Preconditions.checkNotNull(report, "report cannot be null");
        report.validate();
    }

    // region start for all root node level constructs
    @Data
    @ToString
    public static final class ConnectionConfig {
        private String controller;
        private String defaultScope;

        private void validate() {
            Preconditions.checkNotNull(controller, "controller URI cannot be null");
        }
    }

    @Data
    @ToString
    public static final class CreateConfig {
        private List<StreamInfo> streams;
        private ConnectionConfig connection;

        private void validate() {
            Preconditions.checkNotNull(streams, "stream information is missing");
            Preconditions.checkArgument(!streams.isEmpty(), "stream information is missing");
            for (StreamInfo streamInfo: streams) {
                streamInfo.connection = connection;
                streamInfo.validate();
            }
        }
    }

    @Data
    @ToString
    public static final class WriteConfig {
        private String scope;
        private String stream;
        private int noOfWriters;
        private int eventSize;
        private int noOfEvents;
        private boolean useRandomKey;
        private int requestRatePerSec;
        private boolean txnWrites;
        private ConnectionConfig connection;

        private void validate() {
            if (connection.defaultScope == null || connection.defaultScope.length() == 0) {
                Preconditions.checkNotNull(scope, "scope cannot be null for writer");
            }
            Preconditions.checkNotNull(stream, "stream cannot be null for writer");
            Preconditions.checkArgument(noOfWriters > 0, "noOfWriters should be > 0");
            Preconditions.checkArgument(eventSize > MAX_PAD_LENGTH, "eventSize should be > " + MAX_PAD_LENGTH);
            Preconditions.checkArgument(noOfEvents > 0, "noOfEvents should be > 0");
        }
    }

    @Data
    @ToString
    public static final class ReadConfig {
        private int noOfReaders;
        private int eventSize;
        private int noOfEvents;
        List<StreamReadConfig> streams;
        ReaderGroupConfig readerGroup;
        private ConnectionConfig connection;

        private void validate() {
            Preconditions.checkArgument(noOfReaders > 0, "noOfReaders should be > 0");
            Preconditions.checkArgument(eventSize > MAX_PAD_LENGTH, "eventSize should be > " + MAX_PAD_LENGTH);
            Preconditions.checkArgument(noOfEvents > 0, "noOfEvents should be > 0");
            Preconditions.checkNotNull(streams, "missing stream information for read");
            Preconditions.checkArgument(!streams.isEmpty(), "missing stream information for read");
            for (StreamReadConfig streamReadConfig: streams) {
                streamReadConfig.connection = connection;
                streamReadConfig.validate();
            }
            if (readerGroup != null) {
                readerGroup.connection = connection;
                readerGroup.validate();
            }
        }
    }

    @Data
    @ToString
    public static final class Report {
        private String type;
        private List<Params> params;

        private void validate() {
            Preconditions.checkNotNull(type, "type cannot be null");
            String message = "Invalid report type [" + type + "] passed. Supported values are "+ supportedReports();
            Preconditions.checkArgument(supportedReports().contains(type), message);

            if (type.equals(ReportType.pravega.name())) {

                List<String> mandatoryKeys = Arrays.asList("controller", "scope", "stream");
                // make sure the scope, stream and controller configurations are supplied
                Preconditions.checkNotNull(params,
                        "Missing stream configurations required for Pravega report. Required configurations are: " + mandatoryKeys.toString());
                Preconditions.checkArgument(params.size() == 3,
                        "Missing stream configurations required for Pravega report. Required configurations are: " + mandatoryKeys.toString());
                for (Params params: params) {
                    String key = params.getKey();
                    String value = params.getValue();
                    Preconditions.checkNotNull(key, "Missing key from the report:params");
                    Preconditions.checkNotNull(value, "Missing key from the report:params");
                    Preconditions.checkArgument(mandatoryKeys.contains(key),
                            "Missing stream configurations required for Pravega report. Required configurations are: " + mandatoryKeys.toString());
                }
            }
        }
    }
    // region end for all root node level constructs

    // region start for all sub-node level constructs

    @Data
    @ToString
    public static final class StreamInfo {
        private String scope;
        private String stream;
        private ScalingPolicy policy;
        private ConnectionConfig connection;

        private void validate() {
            if (connection.defaultScope == null || connection.defaultScope.length() == 0) {
                Preconditions.checkNotNull(scope, "scope cannot be null for create stream");
            }
            Preconditions.checkNotNull(stream, "stream cannot be null for create stream");
            Preconditions.checkNotNull(policy, "scaling policy cannot be null for create stream");
            policy.validate();
        }
    }

    @Data
    @ToString
    public static final class ScalingPolicy {
        FixedScalingPolicy fixed;
        DynamicScalingPolicy dynamic;

        private void validate() {
            if (fixed == null && dynamic == null) {
                throw new RuntimeException("expecting fixed or dynamic scaling policy");
            }
            if (fixed != null && dynamic != null) {
                throw new RuntimeException("expecting fixed or dynamic scaling policy, not both");
            }
            if (fixed != null) fixed.validate();
            if (dynamic != null) dynamic.validate();
        }
    }

    @Data
    @ToString
    public static final class FixedScalingPolicy {
        private int noOfSegments;

        private void validate() {
            Preconditions.checkArgument(noOfSegments > 0, "noOfSegments should be > 0");
        }
    }

    @Data
    @ToString
    public static final class DynamicScalingPolicy {
        private int targetKBps;//data rate
        private int targetRate; //event rate
        private int scaleFactor;
        private int minNumSegments;

        private void validate() {
            if (targetKBps <= 0 && targetRate <= 0) {
                throw new RuntimeException("expecting targetKBps or targetRate for dynamic scaling policy");
            }
            Preconditions.checkArgument(scaleFactor > 0, "scaleFactor should be > 0");
            Preconditions.checkArgument(minNumSegments > 0, "minNumSegments should be > 0");
        }
    }

    @Data
    @ToString
    public static final class StreamReadConfig {
        private String scope;
        private String stream;
        private String startStreamCut;
        private String endStreamCut;
        private ConnectionConfig connection;

        private void validate() {
            if (connection.defaultScope == null || connection.defaultScope.length() == 0) {
                Preconditions.checkNotNull(scope, "scope cannot be null for read");
            }
            Preconditions.checkNotNull(stream, "stream cannot be null for read");
        }
    }

    @Data
    @ToString
    public static final class ReaderGroupConfig {
        private String name;
        private String scope;
        private ConnectionConfig connection;

        void validate() {
            if (connection.defaultScope == null || connection.defaultScope.length() == 0) {
                Preconditions.checkNotNull(scope, "scope cannot be null for readergroup");
            }
        }
    }

    @Data
    @ToString
    public static final class Params {
        private String key;
        private String value;

    }

    // region end for all sub-node level constructs

    public enum ReportType {
        csv,
        pravega
    }

    public static List<String> supportedReports() {
        return Stream.of(ReportType.values())
                .map(ReportType::name)
                .collect(Collectors.toList());
    }

}