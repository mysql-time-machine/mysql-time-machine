package com.booking.validation.util;

/**
 * Simple utility class for parsing command line options.
 */

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Cmd {
    private static final String DEFAULT_BINLOG_FILENAME_PATERN = "mysql-bin.";

    public static OptionSet parseArgs(String[] args) {

        OptionParser parser = new OptionParser();

        parser.accepts("hbase-namespace").withRequiredArg().ofType(String.class);
        parser.accepts("schema").withRequiredArg().ofType(String.class);
        parser.accepts("hbase-config-path").withRequiredArg().ofType(String.class);
        parser.accepts("kafka-config-path").withRequiredArg().ofType(String.class);

        return parser.parse(args);
    }
}

