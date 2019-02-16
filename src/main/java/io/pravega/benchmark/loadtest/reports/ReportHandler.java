package io.pravega.benchmark.loadtest.reports;

import io.pravega.benchmark.loadtest.utils.AppConfig;

public interface ReportHandler {
    void open(AppConfig appConfig) throws Exception;
    void emit(Stats stats) throws Exception;
    void close();

}

