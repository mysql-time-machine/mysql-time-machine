package com.booking.validation;

import com.booking.validation.util.Cmd;
import com.booking.validation.util.StartupParameters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import joptsimple.OptionSet;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by lezhong on 7/14/16.
 */

public class Main {
    private static Comparator comparator;

    public static void main(String[] args) throws Exception {
        OptionSet optionSet = Cmd.parseArgs(args);
        StartupParameters startupParameters = new StartupParameters(optionSet);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        String hbaseConfigPath = startupParameters.getHBaseConfigPath();
        String kafkaConfigPath = startupParameters.getKafkaConfigPath();
        final ConfigurationHBase confHbase;
        final ConfigurationKafka confKafka;

        try {
            InputStream inHBase = Files.newInputStream(Paths.get(hbaseConfigPath));
            InputStream inKafka = Files.newInputStream(Paths.get(kafkaConfigPath));
            confHbase = mapper.readValue(inHBase, ConfigurationHBase.class);
            confKafka = mapper.readValue(inKafka, ConfigurationKafka.class);

            if (confHbase == null || confKafka == null) {
                throw new RuntimeException(String.format("Unable to load configuration from file: %s", hbaseConfigPath));
            }
            confHbase.loadStartupParameters(startupParameters);
            confKafka.loadStartupParameters(startupParameters);
            confHbase.validate();
            confKafka.validate();
            comparator = new Comparator(confKafka, confHbase);
            // comparator.compareMySQLandKafka();
            comparator.compareMySQLandHBase();
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    } // end main
}
