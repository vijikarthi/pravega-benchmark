package io.pravega.benchmark.loadtest.reports;

import io.pravega.benchmark.loadtest.utils.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CSVReportHandler implements ReportHandler {

    private String fileName;

    private CSVPrinter printer = null;

    @Override
    public void open(AppConfig appConfig) throws Exception {
        if (appConfig.getFileName() != null && appConfig.getFileName().length() > 0) {
            fileName = appConfig.getAppName() + "-" + appConfig.getRunMode().name() + "-" + appConfig.getFileName() + ".csv";
        } else {
            fileName = appConfig.getAppName() + "-" + appConfig.getRunMode().name() + "-" + System.currentTimeMillis() + ".csv";
        }
        FileWriter out = new FileWriter(fileName);
        printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(Stats.headers));
        log.info("opened csv file [{}] for writing", fileName);
    }

    @Override
    public void emit(Stats stats) throws Exception {
        List<String[]> result = new ArrayList<>();
        result.add(stats.data());
        printer.printRecords(result);
    }

    @Override
    public void close() {
        try {
            if (printer != null) {
                printer.close(true);
                log.info("closed csv file [{}]", fileName);
            }
        } catch (Exception e) {
            log.error("csv file close error", e);
        }
    }
}