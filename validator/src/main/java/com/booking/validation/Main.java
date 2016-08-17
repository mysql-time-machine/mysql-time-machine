package com.booking.validation;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by lezhong on 7/14/16.
 */


public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    static final String ANSI_RESET = "\u001B[0m";
    static final String ANSI_GREEN = "\u001B[32m";
    static final String ANSI_RED = "\u001B[31m";
    static final int partitionLength = 1;

    static class Config {
        String host;
        String table;
        String hbaseTable;
        HashMap<String, Boolean> tests;

        Config() {
            host = "";
            table = "";
            hbaseTable = "";
            tests = new HashMap<>();
        }
    }

    public static void compareMySQLandHBase() {
        String username = Configuration.getMySQLUsername();
        String password = Configuration.getPassword();
        String dbName = Configuration.getdbName();
        Config dbConfig = Configuration.get_config(dbName);
        String dataSource = String.format("dbi:mysql:%s;host=%s", dbName, dbConfig.host);
        String dataSourceInfo = String.format("dbi:mysql:information_schema;host=%s", dbConfig.host);

        MySQLConnector dbhInfo = new MySQLConnector(username, password, dbConfig.host);
        ArrayList<MySQLConnector.ColumnTypes> columnTypes = dbhInfo.getColumnTypes(dbName, dbConfig.table);

        // Value Match TODO: validating

        // Hbase Connection

        ArrayList<String> ids = dbhInfo.getIds(dbName, dbConfig.table);

        System.out.println(String.format("Total of %d ids from table %s will be tested, split info %d chunks.",
                ids.size(), dbConfig.table, ids.size() / partitionLength));

        HashMap<String, Integer> stats = new HashMap<>();
        stats.put("COLUMNS_PASS_TOTAL", 0);
        stats.put("COLUMNS_FAIL_TOTAL", 0);
        stats.put("IDS_PASS_TOTAL", 0);
        stats.put("IDS_FAIL_TOTAL", 0);

        // get_tests();
        for (int chunkNo = 0;chunkNo < ids.size(); chunkNo += partitionLength) {
            List<String> chunk = ids.subList(chunkNo, chunkNo + partitionLength);
            // System.out.println(String.format("Processing chunk %d...", chunkNo));
            if (chunkNo == 0) {
                System.out.println(new String(new char[80]).replace('\0', '-'));
                System.out.println(String.format("%sPASS: { rows => %9d, columns => %9d }%s", ANSI_GREEN,
                        stats.get("IDS_PASS_TOTAL"), stats.get(""), ANSI_RESET));
                System.out.println(ANSI_RED + "FAIL: {}" + ANSI_RESET);
            }
            HashMap<String, List<String>> chunkHash = new HashMap<>();
            chunkHash.put("id", chunk);
            HashMap<String, HashMap<String, String>> mySQLRows = dbhInfo.getMySQLRows(dbName, dbConfig.table, chunkHash);
            // HBaseRows = getHBaseRows(chunk);
            for (String id: chunk) {
                HashMap<String, String> myRow = mySQLRows.get(id);
                HashMap<String, String> hbRow = new HashMap<String, String>();
                for (String key: dbConfig.tests.keySet()) {
                    Boolean value = dbConfig.tests.get(key);
                    // getTest(id, MyRow, HBRow);
                }
            }
        }
    }

    public static void compareMySQLandKafka() {
        Validating validator = new Validating();
        String username = Configuration.getMySQLUsername();
        String password = Configuration.getPassword();
        String dbName = Configuration.getdbName();
        Config dbConfig = Configuration.get_config(dbName);
        MySQLConnector dbhInfo = new MySQLConnector(username, password, dbConfig.host);
        HashMap<String, Integer> stats = new HashMap<>();
        stats.put("COLUMNS_PASS_TOTAL", 0);
        stats.put("COLUMNS_FAIL_TOTAL", 0);
        stats.put("IDS_PASS_TOTAL", 0);
        stats.put("IDS_FAIL_TOTAL", 0);

        KafkaConnector kafkaConnector = new KafkaConnector();
        for (int count = 1; count < 20; count ++ ) {
            JSONObject val = kafkaConnector.nextKeyValue();
            String type = val.get("eventType").toString();
            String tableName = val.get("tableName").toString();
            JSONArray pkSet = (JSONArray) val.get("primaryKeyColumns");
            JSONObject eventColumns = (JSONObject) val.get("eventColumns");
            HashMap<String, List<String>> pks = new HashMap<>();
            for (int ind = 0;ind < pkSet.size(); ind ++) {
                String key = pkSet.get(ind).toString();
                List<String> idValue = new ArrayList<>();
                JSONObject valueTuples = (JSONObject) eventColumns.get(key);
                switch (type) {
                    case "UPDATE": {
                        idValue.add(valueTuples.get("value_after").toString());
                    } break;
                    case "INSERT": case "DELETE": {
                        idValue.add(valueTuples.get("value").toString());
                    } break;
                    default: break;
                }
                pks.put(key, idValue);
            }
            HashMap<String, HashMap<String, String>> mySQLRows = dbhInfo.getMySQLRows(dbName, tableName, pks);
            for (String key: mySQLRows.keySet()) {
                HashMap<String, String> mySQLRow = mySQLRows.get(key);
                switch (type) {
                    case "UPDATE": {
                        for (Object columnKey : eventColumns.keySet()) {
                            JSONObject kafkaValue = (JSONObject) eventColumns.get(columnKey);
                            String valueType = kafkaValue.get("type").toString();
                            String valueFromMySQL = mySQLRow.get(columnKey.toString());
                            String valueFromKafka = kafkaValue.get("value_after").toString();
                            Boolean res = validator.comparisonHelper(valueType, valueFromMySQL, valueFromKafka);
                            if (!res) {
                                System.out.println(String.format("id: %s, column: %s, value: %s != %s", key,
                                        columnKey, valueFromMySQL, valueFromKafka));
                            } else {
                                System.out.println("Correct!");
                            }
                        }
                        break;
                    }
                    case "INSERT": {
                        for (Object columnKey : eventColumns.keySet()) {
                            JSONObject kafkaValue = (JSONObject) eventColumns.get(columnKey);
                            String valueType = kafkaValue.get("type").toString();
                            String valueFromMySQL = mySQLRow.get(columnKey.toString());
                            String valueFromKafka = kafkaValue.get("value").toString();
                            Boolean res = validator.comparisonHelper(valueType, valueFromMySQL, valueFromKafka);
                            if (!res) {
                                System.out.println(String.format("id: %s, column: %s, value: %s != %s", key,
                                        columnKey, valueFromMySQL, valueFromKafka));
                            } else {
                                System.out.println("Correct!");
                            }
                        }
                        break;
                    }
                    case "DELETE": {
                        for (Object columnKey : eventColumns.keySet()) {
                            JSONObject kafkaValue = (JSONObject) eventColumns.get(columnKey);
                            String valueFromMySQL = mySQLRow.get(columnKey.toString());
                            Boolean res = valueFromMySQL == null;
                            if (!res) {
                                System.out.println(String.format("id: %s, column: %s, value: %s != %s", key,
                                        columnKey, valueFromMySQL));
                            } else {
                                System.out.println("Correct!");
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // compareMySQLandHBase();
        compareMySQLandKafka();
    } // end main
}
