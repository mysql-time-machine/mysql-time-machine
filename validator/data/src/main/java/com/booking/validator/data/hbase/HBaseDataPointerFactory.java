package com.booking.validator.data.hbase;


import com.booking.validator.data.DataPointer;
import com.booking.validator.data.DataPointerFactory;
import com.booking.validator.data.storage.KeyValueStorageDataPointer;
import com.booking.validator.data.hbase.storage.HBaseKey;
import com.booking.validator.data.hbase.storage.HBaseKeyValueStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by psalimov on 9/13/16.
 */
public class HBaseDataPointerFactory implements DataPointerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDataPointerFactory.class);

    private enum Property {
        CLUSTER("cluster"), TABLE("table"), ROW("row"), COLUMN_FAMILY("cf");

        private final String name;

        Property(String name) { this.name = name; }

        public String value(Map<String, String> values){

            String value = values.get(name);

            if (value == null) throw new InvalidDataPointerDescription("Property " + name + " is not defined");

            return value;
        }

    }

    private final Map<String, Connection> clusters;

    public static HBaseDataPointerFactory getInstance(Map<String,Configuration> clusterConfigurations){

        Map<String, Connection> clusters = new HashMap<>();

        for ( Map.Entry<String,Configuration> clusterAndConfig : clusterConfigurations.entrySet() ){

            try {

                Connection connection = ConnectionFactory.createConnection( clusterAndConfig.getValue() );

                clusters.put( clusterAndConfig.getKey(), connection );

            } catch (IOException e) {

                LOGGER.error("Failed to create hbase connection", e);

            }

        }


        return new HBaseDataPointerFactory( clusters );

    }

    public HBaseDataPointerFactory(Map<String, Connection> clusters){
        this.clusters = clusters;
    }


    @Override
    public DataPointer produce(Map<String, String> storageDescription, Map<String, String> keyDescription) {

        Connection connection = clusters.get( Property.CLUSTER.value(storageDescription) );

        if (connection == null) throw new InvalidDataPointerDescription("Unknown cluster");

        HBaseKeyValueStorage storage = new HBaseKeyValueStorage(connection, Property.TABLE.value(storageDescription) );

        HBaseKey key = new HBaseKey(Bytes.toBytes( Property.ROW.value(keyDescription) ),Bytes.toBytes( Property.COLUMN_FAMILY.value(keyDescription) ));

        return new KeyValueStorageDataPointer<>(storage,key);
    }
}
