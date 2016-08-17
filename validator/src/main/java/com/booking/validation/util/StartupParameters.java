package com.booking.validation.util;

import joptsimple.OptionSet;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

/**
 * Created by bdevetak on 01/12/15.
 */

public class StartupParameters {

    private String  configPath;
    private String  schema;
    private String  hbaseNamespace;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(StartupParameters.class);

    public StartupParameters(OptionSet optionSet) {

        // schema
        if (optionSet.hasArgument("schema")) {
            schema = optionSet.valueOf("schema").toString();
        } else {
            schema = "test";
        }

        // config-path
        configPath = (String) optionSet.valueOf("config-path");

        // setup hbase namespace
        hbaseNamespace = (String) optionSet.valueOf("hbase-namespace");

        System.out.println("----------------------------------------------");
        System.out.println("Parsed params:           ");
        System.out.println("\tconfig-path:           " + configPath);
        System.out.println("\tschema:                " + schema);
        System.out.println("\thbase-namespace:       " + hbaseNamespace);
        System.out.println("----------------------------------------------\n");

    }

    public String getConfigPath() {
        return configPath;
    }

    public String getSchema() {
        return schema;
    }

    public String getHbaseNamespace() {
        return hbaseNamespace;
    }
}
