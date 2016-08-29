package com.booking.validation;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by lezhong on 8/15/16.
 */

public class HBaseIDFetcher {
    static ConfigurationHBase configurationHBase = new ConfigurationHBase();

    static String extractPrimaryIds(String val, String tableName) {
        String ans = "";
        JSONParser parser = new JSONParser();
        try {
            JSONObject valObj = (JSONObject) parser.parse(val);
            JSONObject schema = (JSONObject) valObj.get(tableName);
            JSONObject columnsSchema = (JSONObject) schema.get("columnsSchema");
            for (Object key : columnsSchema.keySet()) {
                JSONObject value = (JSONObject) columnsSchema.get(key);
                String str = value.get("columnKey").toString();
                if (str.equals("PRI")) {
                    if (ans.length() == 0) {
                        ans = key.toString();
                    } else {
                        ans += String.format("%s, %s", ans, key);
                    }
                }
            }
        } catch (ParseException pa) {
            pa.printStackTrace();
        }
        return ans;
    }

    static String getTablePrimaryID(String schemaName, String tableName) {
        String val;

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", configurationHBase.getHBaseZKQuorum());
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf(schemaName));
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange"));
            ResultScanner scanner = table.getScanner(scan);
            try {
                Result re = scanner.next();
                val = new String(re.getValue(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange")));
                return extractPrimaryIds(val, tableName);
            } catch (Exception ex) {
                ex.printStackTrace();;
            } finally {
                scanner.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return "id"; // By default
    }
}
