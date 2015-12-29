package com.booking.replication.util;

import joptsimple.OptionSet;
import org.apache.commons.cli.MissingArgumentException;

/**
 * Created by bdevetak on 01/12/15.
 */
public class StartupParameters {

    private String configPath;
    private String dc;
    private String schema;
    private String applier;
    private String binlogFileName;
    private Long   binlogPosition;
    private Integer shard;

    private static final String DEFAULT_BINLOG_FILENAME_PATERN = "mysql-bin.";

    public void init(OptionSet o) throws MissingArgumentException {

        // dc
        if (o.hasArgument("dc")) {
            dc = o.valueOf("dc").toString();
        }
        else {
            dc = "dc1";
        }

        // schema
        if (o.hasArgument("schema")) {
            schema = o.valueOf("schema").toString();
        }
        else {
            schema = "test";
        }

        // shard
        if (o.hasArgument("shard")) {
            shard = Integer.parseInt(o.valueOf("shard").toString());
        }
        else {
            shard = 0;
        }

        // config-path
        if (o.hasArgument("config-path")) {
            configPath = o.valueOf("config-path").toString();
        }
        else {
            configPath = "./config.yml";
        }

        // applier, defaults to STDOUT
        if (o.hasArgument("applier")) {
            applier = o.valueOf("applier").toString();
        }
        else {
            applier = "STDOUT";
        }

        // binlog-filename
        if (o.hasArgument("binlog-filename")) {
            binlogFileName = o.valueOf("binlog-filename").toString();
        }
        else {
            binlogFileName = DEFAULT_BINLOG_FILENAME_PATERN + "000001";
        }

        // position
        if (o.hasArgument("position")) {
            binlogPosition = Long.parseLong(o.valueOf("position").toString());
        }
        else {
            // default to 4
            binlogPosition = 4L;
        }

        System.out.println("----------------------------------------------");
        System.out.println("Parsed params:     ");
        System.out.println("\tconfig-path:     " + configPath);
        System.out.println("\tdc:              " + dc);
        System.out.println("\tschema:          " + schema);
        System.out.println("\tapplier:         " + applier);
        System.out.println("\tbinlog-filename: " + binlogFileName);
        System.out.println("\tposition:        " + binlogPosition);
        System.out.println("----------------------------------------------\n");

    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getApplier() {
        return applier;
    }

    public void setApplier(String applier) {
        this.applier = applier;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    public Long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(Long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public Integer getShard() {
        return shard;
    }

    public void setShard(Integer shard) {
        this.shard = shard;
    }
}
