package com.booking.replication.applier;

import com.booking.replication.Constants;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.CurrentTransactionMetadata;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
    private static final int EVENT_BUFFER_SIZE = 20;
    private static final int POOL_SIZE = 10;
    private static final String DIGEST_ALGORITHM = "MD5";


    private static final Logger        LOGGER = LoggerFactory.getLogger(HBaseApplier.class);

    private static final Configuration hbaseConf = HBaseConfiguration.create();

    private static final byte[] CF = Bytes.toBytes("d");


    private static String currentTransactionUUID;

    private static long startTime = 0;

    private static long eventRows_BufferProcessingTime = 0;

    private static ExecutorService workerPool;

    private final static Map<String, Future<Integer>> tasksInProgress = new HashMap<String,Future<Integer>>();


    /**
     * Concurrent transaction-based mutations buffer:
     *
     * Each transaction buffer is identified with UUID. Buffers are picked up by flusher threads and on success the UUID key
     * is deleted from the the buffer. On failure, status is updated to 'FAILED'. The hash structure looks like
     * this:
     *
     *  {
     *      "874c3466-3bf0-422f-a3e3-148289226b6c" => {
     *
     *        table_1 => [@table_1_mutations]
     *        ,...,
     *        table_N => [@table_N_mutations]
     *
     *      },
     *
     *      "187433e5-7b05-47ff-a3bd-633897cd2b4f" => {
     *
     *        table_1 => [@table_1_mutations]
     *        ,...,
     *        table_N => [@table_N_mutations]
     *
     *      },
     *  }
     *
     * This works asynchronously for maximum performance. Since transactions are timestamped and they are from RBR
     * we can apply them in any order. In HBase all of them will be present with corresponding timestamp. And RBR
     * guaranties that each operation is idempotent (so there is no queries that transform data like update value
     * to value * x, which would break the idempotent feature of operations). Simply put, the order of applying of
     * different transactions does not influence the end result since data will be timestamped with timestamps
     * from the binlog and all versions are kept.
     */
    private Map<String, Map<String,List<Mutation>>> uuidMutationBuffers = new HashMap<String, Map<String,List<Mutation>>>();
    private ConcurrentHashMap<String, Integer> uuidMutationBufferStatus = new ConcurrentHashMap<String,Integer>();

    private int numberOfCachedEvents = 0;
    private int numberOfBufferedRows = 0;

    private Connection hbaseConnection;

    public HBaseApplier(String ZOOKEEPER_QUORUM) throws IOException {

        hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

        hbaseConnection = ConnectionFactory.createConnection(hbaseConf);

        // System.setProperty("hadoop.home.dir", "/");

        currentTransactionUUID = UUID.randomUUID().toString();
        uuidMutationBuffers.put(currentTransactionUUID, new HashMap<String,List<Mutation>>() );
        uuidMutationBufferStatus.put(currentTransactionUUID, UUIDBufferStatus.READY_FOR_BUFFERING);

        workerPool = Executors.newFixedThreadPool(POOL_SIZE);

    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event, PipelineOrchestrator caller) {
        // This is just one COMMIT statement.
        //LOGGER.info("COMMIT query event.");
        submitCurrentTransaction();
        caller.currentTransactionMetadata = null;
        caller.currentTransactionMetadata = new CurrentTransactionMetadata();
    }

    @Override
    public void applyXIDEvent(XidEvent event, PipelineOrchestrator caller) {
        // TODO: one uuid identifies one transaction, so when XID
        //       is received we can submit that uuid buffer
        //       and create new currentTransactionID
        //LOGGER.info("XID event.");
        // TODO: add transactionID to storage
        long transactionID = event.getXid();
        submitCurrentTransaction();
        caller.currentTransactionMetadata = null;
        caller.currentTransactionMetadata = new CurrentTransactionMetadata();
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent e, PipelineOrchestrator caller) {

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

    @Override
    public void apply(AugmentedRowsEvent augmentedRowsEvent, PipelineOrchestrator context) {

        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        // 1. getValue database_name from event

        String mySQLDBName = context.configuration.getReplicantSchemaName(); // TODO: add this to AugmentedRow

        String currentTransactionDB = context.currentTransactionMetadata.getFirstMapEventInTransaction().getDatabaseName().toString();

        String hbaseNamespace = null;
        if (currentTransactionDB != null) {
            // LOGGER.info("currentTransactionDB => " + currentTransactionDB);

            if (currentTransactionDB.equals(mySQLDBName)) {
                // regular data
                // LOGGER.info("currentTransactionDB => " + currentTransactionDB);
                hbaseNamespace = mySQLDBName.toLowerCase();
            }
            else if(currentTransactionDB.equals(Constants.BLACKLISTED_DB)) {
                // meta data
                if (context.configuration.getReplicantShardID() == 0) {
                    // not sharded
                    hbaseNamespace = mySQLDBName.toLowerCase()
                            + "_"
                            + Constants.BLACKLISTED_DB;
                    // LOGGER.info("Not sharded, metadata namespace => " + hbaseNamespace);
                } else if (context.configuration.getReplicantShardID() > 0){
                    // sharded
                    hbaseNamespace = mySQLDBName.toLowerCase()
                            + String.valueOf(context.configuration.getReplicantShardID())
                            + "_"
                            + Constants.BLACKLISTED_DB;
                    // LOGGER.info("Sharded, metadata namespace => " + hbaseNamespace);
                }
                else {
                    // LOGGER.info("Invalid shard index => " + context.configuration.getReplicantShardID());
                }
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

        long tStart = System.currentTimeMillis();

        // 2. prepare and buffer Put objects for all rows in the received event
        for (AugmentedRow row : augmentedRowsEvent.getSingleRowEvents()) {
            // 2. getValue table_name from event
            String mySQLTableName = row.getTableName();
            String hbaseTableName = hbaseNamespace + ":" + mySQLTableName.toLowerCase();

            //try {

            // RowID
            List<String> pkColumnNames = row.getPrimaryKeyColumns(); // <- this is sorted by OP
            List<String> pkColumnValues = new ArrayList<String>();

            for (String pkColumnName : pkColumnNames) {

                Map<String, String> pkCell = row.getEventColumns().get(pkColumnName);

                if (row.getEventType().equals("INSERT") || row.getEventType().equals("DELETE")) {
                    pkColumnValues.add(pkCell.get("value"));
                } else if (row.getEventType().equals("UPDATE")) {
                    pkColumnValues.add(pkCell.get("value_after"))
                    ;
                } else {
                    LOGGER.error("Wrong event type. Expected RowType event.");
                }
            }

            String hbaseRowID = Joiner.on(";").join(pkColumnValues);
            String saltingPartOfKey = pkColumnValues.get(0);

            // avoid region hotspotting
            hbaseRowID = saltRowKey(hbaseRowID, saltingPartOfKey);

            Put p = new Put(Bytes.toBytes(hbaseRowID));

            // Columns
            for (String columnName : row.getEventColumns().keySet()) {

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue = "";

                if (row.getEventType().equals("INSERT") || row.getEventType().equals("DELETE")) {

                    columnValue = row.getEventColumns().get(columnName).get("value");
                    if (columnValue == null) {
                        columnValue = "NULL";
                    }
                } else if (row.getEventType().equals("UPDATE")) {
                    columnValue = row.getEventColumns().get(columnName).get("value_after");
                    if (columnValue == null) {
                        columnValue = "NULL";
                    }
                } else {
                    LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                    System.exit(1);
                }
                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
            }

            // Push to buffer
            //pushToMutationBuffer(hbaseTableName, p);
            pushToUUIDBuffer(currentTransactionUUID, hbaseTableName, p);

            numberOfBufferedRows++;

        } // next row

        numberOfCachedEvents++;

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        eventRows_BufferProcessingTime += tDelta;

        // 3. flush buffered rows: on every EVENT_BUFFER_SIZE events buffered, call flush
        // TODO: restrict EVENT_BUFFER_SIZE to work only in initial snapshot mode
        if (numberOfCachedEvents >= EVENT_BUFFER_SIZE) {

            LOGGER.info("Filled current uuidBuffer " + currentTransactionUUID
                    + " { numberOfCachedEvents => " + numberOfCachedEvents
                    + ", numberOfBufferedRows => " + numberOfBufferedRows
                    + "} in"
                    + eventRows_BufferProcessingTime
                    + " ms"
            );

            eventRows_BufferProcessingTime = 0;

            submitCurrentTransaction();

            long now = System.currentTimeMillis();
            long totalDiff = now - startTime;

            double rate = (double) context.consumerStatsNumberOfProcessedRows / (((double) totalDiff) / 1000);

            LOGGER.info("Applier stats so far: {" +
                    " eventsRead => " + context.consumerStatsNumberOfProcessedEvents
                    + ", rowsContained => " + context.consumerStatsNumberOfProcessedRows
                    + ", total time elapsed => " + (((double) totalDiff) / 1000) + " seconds "
                    + ", average ingestion rate => " + rate + " rows/seconds"
                    + " }"
            );

            // check if some of the running tasks has finished
            // check futures for results
            Map<String, Integer> doneTasks = new HashMap<String, Integer>();

            for (String submittedUUID : tasksInProgress.keySet()) {
                Future<Integer> f = tasksInProgress.get(submittedUUID);
                if (f.isDone()) {
                    try {
                        Integer result = f.get();
                        if (result == UUIDBufferStatus.WRITE_SUCCEEDED) {
                            // uuid buffer flushed, remove from buffers map
                            doneTasks.put(submittedUUID, UUIDBufferStatus.WRITE_SUCCEEDED);

                        } else if (result == UUIDBufferStatus.WRITE_FAILED) {
                            // reschedule
                            doneTasks.put(submittedUUID,  UUIDBufferStatus.WRITE_FAILED);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }

            for (String taskUUID : doneTasks.keySet()) {
                if (doneTasks.get(taskUUID) == UUIDBufferStatus.WRITE_SUCCEEDED) {
                    uuidMutationBufferStatus.remove(taskUUID);
                    uuidMutationBuffers.remove(taskUUID);
                }
                else {
                    // keep the buffer, just change the status so this task is picked up again
                    uuidMutationBufferStatus.put(taskUUID, UUIDBufferStatus.READY_FOR_PICK_UP);

                }
                tasksInProgress.remove(taskUUID);
            }
        }
    }

    private void submitCurrentTransaction() {

        // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
        markCurrentAsReadyAndCreateNewUUIDBuffer();

        LOGGER.info("marked " + currentTransactionUUID + " as ready for flush");

        submitMarkedBuffersInParallel(); // <- current and those which previously failed

        LOGGER.info("Created futures");

        numberOfCachedEvents = 0;
        numberOfBufferedRows = 0;

    }

    private void pushToUUIDBuffer(String uuid, String tableName, Put put) {

        // LOGGER.info("pushToUUIDBuffer tableName => " + tableName);

        if (uuidMutationBuffers.get(uuid) == null) {
            LOGGER.error("ERROR: Missing UUID from uuidBuffer keySet. Should never happen. Shutting down...");
            System.exit(1);
        }
        if (uuidMutationBuffers.get(uuid).get(tableName) == null) {
            uuidMutationBuffers.get(uuid).put(tableName,new ArrayList<Mutation>());
            // LOGGER.info("New table in uuid buffer " + uuid + ", initialized key for table " + tableName );
        }
        uuidMutationBuffers.get(uuid).get(tableName).add(put);
    }

    private void markCurrentAsReadyAndCreateNewUUIDBuffer() {

        String newUUID = UUID.randomUUID().toString();

        uuidMutationBufferStatus.put(currentTransactionUUID, UUIDBufferStatus.READY_FOR_PICK_UP);

        currentTransactionUUID = newUUID;
        uuidMutationBuffers.put(currentTransactionUUID, new HashMap<String,List<Mutation>>() );
        uuidMutationBufferStatus.put(currentTransactionUUID, UUIDBufferStatus.READY_FOR_BUFFERING);

    }

    private void submitMarkedBuffersInParallel() {

        byte[] FAMILY = Bytes.toBytes("d");

        if (hbaseConnection == null) {
            int retry = 10;
            while (retry > 0) {
                try {
                    hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
                    retry = 0;
                } catch (IOException e) {
                    LOGGER.info("Failed to create hbase connection, attempt " + retry + "/10");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.info("Thread wont sleep. Not a good day for you.");
                    e.printStackTrace();
                }
                retry--;
            }
        }

        // add tasks to futures list
        for (final String bufferUUID : uuidMutationBufferStatus.keySet()) {
            if (uuidMutationBufferStatus.get(bufferUUID) == UUIDBufferStatus.READY_FOR_PICK_UP) {
                for (final String bufferedTableName : uuidMutationBuffers.get(bufferUUID).keySet()) {

                    // LOGGER.info("bufferetedTableName1 => " + bufferedTableName);

                    TableName TABLE = TableName.valueOf(bufferedTableName);

                    // LOGGER.info("bufferetedTableName2 => " + bufferedTableName);

                    // create listener
                    BufferedMutator.ExceptionListener listener =
                        new BufferedMutator.ExceptionListener() {
                            @Override
                            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                                for (int i = 0; i < e.getNumExceptions(); i++) {
                                    LOGGER.info("Failed to sent put: " + e.getRow(i));
                                }
                            }
                        };

                    // attach listener
                    BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

                    try {
                        // getValue mutator
                        final BufferedMutator mutator = hbaseConnection.getBufferedMutator(TABLE);

                        uuidMutationBufferStatus.put(bufferUUID, UUIDBufferStatus.WRITE_IN_PROGRESS);

                        tasksInProgress.put(bufferUUID, workerPool.submit(new Callable<Integer>() {
                            @Override
                            public Integer call() {
                                try {
                                    mutator.mutate(uuidMutationBuffers.get(bufferUUID).get(bufferedTableName)); // <<- List<Mutation> for the given table
                                    mutator.flush();
                                    mutator.close();

                                    return UUIDBufferStatus.WRITE_SUCCEEDED;
                                }
                                catch (IOException e) {
                                    LOGGER.error("Failed to flush buffer for table " + bufferedTableName + " in uuidBuffer " + bufferUUID + ". Marking this uuid for retry...");
                                    uuidMutationBufferStatus.put(bufferUUID, UUIDBufferStatus.WRITE_FAILED);
                                    return UUIDBufferStatus.WRITE_FAILED;
                                }
                            }
                        }));

                    } catch (IOException e) {
                        LOGGER.error("ERROR: Failed to apply mutations for table " + bufferedTableName + " in uuidBuffer " + bufferUUID);
                        e.printStackTrace();
                    }
                }
            }
        }
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
