package com.booking.validator.data.hbase.storage;

import com.booking.validator.data.storage.KeyValueStorage;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Created by psalimov on 9/8/16.
 */
public class HBaseKeyValueStorage implements KeyValueStorage<HBaseKey, HBaseValue> {

    private final Connection connection;
    private final String tableName;

    public HBaseKeyValueStorage(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public HBaseValue get(HBaseKey key) {

        try ( Table table = connection.getTable(TableName.valueOf(tableName) ) ){

            Get get = new Get( key.row() );

            Result result = table.get(get);

            if (result.isEmpty()) return null;

            // TODO: allow the key to specify the version (time)

            if (key.family() != null){

                NavigableMap resultMap = result.getFamilyMap( key.family() );

                return new HBaseValue(resultMap);

            } else {

                throw new UnsupportedOperationException("HBase key does not specify the column family");

            }

        } catch (IOException e) {

            throw new RuntimeException(e);

        }

    }
}
