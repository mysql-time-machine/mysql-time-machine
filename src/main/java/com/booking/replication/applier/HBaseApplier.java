package com.booking.replication.applier;

import com.booking.replication.Constants;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import java.security.MessageDigest;

/**
 * This class abstracts the HBase store.
 *
 * Conventions used:
 *
 *      1. Each replication chain is replicated to a namespace "${chain_name}_replication".
 *
 *      2. All table names are converted to low-caps. For example My_Schema.My_Table will be replicated
 *         to 'my_schema:my_table'
 */
public class HBaseApplier implements Applier {

    // TODO: move configuration vars to Configuration
    private static final int UUID_BUFFER_SIZE = 500; // <- max number of rows in one uuid buffer
    private static final int POOL_SIZE = 10;

    private static final String DIGEST_ALGORITHM = "MD5";

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplier.class);

    private static final Configuration hbaseConf = HBaseConfiguration.create();

    private static final byte[] CF = Bytes.toBytes("d");

    private final ReplicatorMetrics replicatorMetrics;

    private final HBaseApplierTaskManager hbaseApplierTaskManager;

    /**
     * HBaseApplier constructor
     *
     * @param ZOOKEEPER_QUORUM
     * @param repMetrics
     * @throws IOException
     */
    public HBaseApplier(String ZOOKEEPER_QUORUM, ReplicatorMetrics repMetrics) throws IOException {
        hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        this.replicatorMetrics = repMetrics;
        hbaseApplierTaskManager = new HBaseApplierTaskManager(POOL_SIZE, repMetrics, hbaseConf);
    }

    /**
     * Applier interface methods
     *
     *  applyCommitQueryEvent
     *  applyXIDEvent
     *  applyAugmentedSchemaChangeEvent
     *
     *  bufferData
     *
     * @param event
     */

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyXIDEvent(XidEvent event) {
        // TODO: add transactionID to storage
        // long transactionID = event.getXid();
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent e, PipelineOrchestrator caller) {

        // TODO:

        // 1. read database_name

        // 2. read table_name

        // 3. read sql_statement

        // 4. read old/new jsons

        // 5. read old/new creates

        // 6. construct Put object with:
        //      row_key = 'database_name;event_timestamp'

        // 7. Write to table:
        //      'schema_replication:schema_version_history'
    }

    /**
     * Core logic of the applier. Processes data events and writes to HBase.
     *
     * @param augmentedRowsEvent
     * @param pipeline
     */
    @Override
    public void bufferData(AugmentedRowsEvent augmentedRowsEvent, PipelineOrchestrator pipeline) {

        // 1. getValue database_name from event
        String mySQLDBName = pipeline.configuration.getReplicantSchemaName();

        String currentTransactionDB = pipeline.currentTransactionMetadata.getFirstMapEventInTransaction().getDatabaseName().toString();

        String hbaseNamespace = null;
        if (currentTransactionDB != null) {

            if (currentTransactionDB.equals(mySQLDBName)) {
                // regular data
                hbaseNamespace = mySQLDBName.toLowerCase();
            }
            else if(currentTransactionDB.equals(Constants.BLACKLISTED_DB)) {
                // skipping blacklisted db
                return;
            }
            else {
                LOGGER.error("Invalid database name: " + currentTransactionDB);
            }
        }
        else {
            LOGGER.error("CurrentTransactionDB can not be null");
        }

        if (hbaseNamespace == null) {
            LOGGER.error("Namespace can not be null");
        }

        // 2. prepare and buffer Put objects for all rows in the received event
        for (AugmentedRow row : augmentedRowsEvent.getSingleRowEvents()) {

            // vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

            // 2. getValue table_name from event
            String mySQLTableName = row.getTableName();
            String hbaseTableName = hbaseNamespace + ":" + mySQLTableName.toLowerCase();

            Pair<String,Put> idPut = getPut(row);

            String id = idPut.getFirst();
            Put p = idPut.getSecond();

            // Push to buffer
            hbaseApplierTaskManager.pushMutationToTaskBuffer(hbaseTableName, id, p);


            // TODO: write same row to delta table if --delta option is on

            // Delta tables have 2 important differences:
            //
            // 1. Columns have only 1 version
            //
            // 2. we are storing entire row (insted only the changes columns - since 1.)
            //
            // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


        } // next row

        // Flush on buffer size limit
        if (hbaseApplierTaskManager.rowsBufferedCounter.get() >= UUID_BUFFER_SIZE) {
            hbaseApplierTaskManager.flushCurrentTaskBuffer();
            hbaseApplierTaskManager.rowsBufferedCounter.set(0);
        }
    }

    private Pair<String,Put> getPut(AugmentedRow row) {

        // RowID
        List<String> pkColumnNames = row.getPrimaryKeyColumns(); // <- this is sorted by column OP
        List<String> pkColumnValues = new ArrayList<String>();

        // LOGGER.info("table => " + mySQLTableName + ", pk columns => " + Joiner.on(";").join(pkColumnNames));

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getEventColumns().get(pkColumnName);

            if (row.getEventType().equals("INSERT") || row.getEventType().equals("DELETE")) {
                pkColumnValues.add(pkCell.get("value"));
            } else if (row.getEventType().equals("UPDATE")) {
                pkColumnValues.add(pkCell.get("value_after"));
            } else {
                LOGGER.error("Wrong event type. Expected RowType event.");
            }
        }

        String hbaseRowID = Joiner.on(";").join(pkColumnValues);
        String saltingPartOfKey = pkColumnValues.get(0);

        // avoid region hot-spotting
        hbaseRowID = saltRowKey(hbaseRowID, saltingPartOfKey);

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        if (row.getEventType().equals("DELETE")) {

            // No need to process columns on DELETE. Only write delete marker.

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnName  = "row_status";
            String columnValue = "D";
            p.addColumn(
                    CF,
                    Bytes.toBytes(columnName),
                    columnTimestamp,
                    Bytes.toBytes(columnValue)
            );
        }
        else if (row.getEventType().equals("UPDATE")) {

            // Only write values that have changed

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                String valueBefore = row.getEventColumns().get(columnName).get("value_before");
                String valueAfter  = row.getEventColumns().get(columnName).get("value_after");

                if ((valueAfter == null) && (valueBefore == null)) {
                    // no change, skip;
                }
                else if (
                        ((valueBefore == null) && (valueAfter != null))
                        ||
                        ((valueBefore != null) && (valueAfter == null))
                        ||
                        (!valueAfter.equals(valueBefore))) {

                    columnValue = valueAfter;
                    p.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }
                else {
                    // no change, skip
                }
            }

            p.addColumn(
                    CF,
                    Bytes.toBytes("row_status"),
                    columnTimestamp,
                    Bytes.toBytes("U")
            );
        }
        else if (row.getEventType().equals("INSERT")) {

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                columnValue = row.getEventColumns().get(columnName).get("value");
                if (columnValue == null) {
                    columnValue = "NULL";
                }

                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
            }

            p.addColumn(
                CF,
                Bytes.toBytes("row_status"),
                columnTimestamp,
                Bytes.toBytes("I")
            );
        }
        else {
            LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
            System.exit(1);
        }

        Pair<String,Put> idPut = new Pair<>(hbaseRowID,p);
        return idPut;
    }

    private void markCurrentTransactionForCommit() {
        hbaseApplierTaskManager.markCurrentTransactionForCommit();
    }


    /**
     * Salting the row keys with hex representation of first two bytes of md5:
     *      hbaseRowID = md5(hbaseRowID)[0] + md5(hbaseRowID)[1] + "-" + hbaseRowID;
     */
     private String saltRowKey(String hbaseRowID, String firstPartOfRowKey) {

         byte[] bytesOfSaltingPartOfRowKey = firstPartOfRowKey.getBytes(StandardCharsets.US_ASCII);

         MessageDigest md = null;
         try {
            md = MessageDigest.getInstance(DIGEST_ALGORITHM);
         } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            LOGGER.error("md5 algorithm not available. Shutting down...");
            System.exit(1);
         }
         byte[] bytes_md5 = md.digest(bytesOfSaltingPartOfRowKey);

         String byte_1_hex = Integer.toHexString(bytes_md5[0] & 0xFF);
         String byte_2_hex = Integer.toHexString(bytes_md5[1] & 0xFF);

         //String salt = "00".substring(0,2 - byte_1_hex.length()) + byte_1_hex
         //            + "00".substring(0,2 - byte_2_hex.length()) + byte_2_hex;

         // add 0-padding
         String salt = ("00" + byte_1_hex).substring(byte_1_hex.length())
                     + ("00" + byte_2_hex).substring(byte_2_hex.length());

         String saltedRowKey = salt + "-" + hbaseRowID;

         return saltedRowKey;
    }

}
