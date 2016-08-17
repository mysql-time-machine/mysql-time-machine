package com.booking.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lezhong on 8/10/16.
 */

public class HBaseConnector {
    private Configuration config;
    private HTable table;
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnector.class);

    HBaseConnector() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", com.booking.validation.Configuration.getHBaseZKQuorum());
    }

    void fetchID() {

    }

    void getHBaseRows() {
        // TODO
    }
}
