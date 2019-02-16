package io.pravega.benchmark.loadtest.reports;

import com.google.common.base.Preconditions;
import io.pravega.benchmark.loadtest.utils.AppConfig;

import static io.pravega.benchmark.loadtest.utils.AppConfig.supportedReports;

public class ReportFactory {

    public static ReportHandler getReportHandler(AppConfig appConfig) {

        Preconditions.checkNotNull(appConfig);
        Preconditions.checkNotNull(appConfig.getReport());
        Preconditions.checkNotNull(appConfig.getReport().getType());

        if (appConfig.getReport().getType().equals(AppConfig.ReportType.csv.name())) {
            return new CSVReportHandler();
        }

        String message = "Invalid report type [" + appConfig.getReport().getType() + "] passed. Supported values are "+ supportedReports();
        throw new UnsupportedOperationException(message);
    }
}
