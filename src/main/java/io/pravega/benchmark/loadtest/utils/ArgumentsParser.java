package io.pravega.benchmark.loadtest.utils;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public final class ArgumentsParser {

    private static String APP_CONFIG_FILE_NAME = "app.json";

    public static AppConfig parseArgs(String ... args) throws Exception {

        Options options = new Options();

        //run mode option
        Option runModeOption = new Option("runMode", true, "specify the run mode. Possible values are: " + supportedRunMode());
        options.addOption(runModeOption);

        //configuration directory option from where app.json is available
        options.addOption("confDir", true, "configuration directory from where to load the app configurations file (app.json)");

        //help option
        options.addOption("help", false, "help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("loadtest", options);
            System.exit(1);
        }

        if (!cmd.hasOption("runMode")) {
            log.error("missing -runMode mandatory argument");
            new HelpFormatter().printHelp("loadtest", options);
            System.exit(1);
        }

        if (!cmd.hasOption("confDir")) {
            log.error("missing -confDir mandatory argument");
            new HelpFormatter().printHelp("loadtest", options);
            System.exit(1);
        }

        String runMode = cmd.getOptionValue("runMode");
        List<String> runModeOptions = supportedRunMode();
        if (!runModeOptions.contains(runMode)) {
            log.error("Unsupported run mode [{}] provided. Supported values are: {}", runMode, runModeOptions);
            new HelpFormatter().printHelp("loadtest", options);
            System.exit(1);
        }


        String configDir = cmd.getOptionValue("confDir");

        Path filePath = Paths.get(configDir + File.separator + APP_CONFIG_FILE_NAME);
        if (!filePath.toFile().exists()) {
            log.error("could not locate the file [{}] from [{}]", filePath.getFileName(), filePath.getParent());
            new HelpFormatter().printHelp("loadtest", options);
            System.exit(1);
        }

        byte[] configBytes = Files.readAllBytes(filePath);
        String config = new String(configBytes);
        log.info("application configurations : {}", config);

        Gson gson = new Gson();
        AppConfig appConfig = gson.fromJson(config, AppConfig.class);
        appConfig.setRunMode(RunMode.valueOf(runMode));
        appConfig.validate();

        log.info("application configurations parsed: {}", appConfig);

        return appConfig;
    }

    public static List<String> supportedRunMode() {
        return Stream.of(RunMode.values())
                .map(RunMode::name)
                .collect(Collectors.toList());
    }

    public enum RunMode {
        create,
        write,
        read
    }
}
