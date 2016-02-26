package com.booking.replication.applier;

import com.booking.replication.metrics.ReplicatorMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HBaseApplierTaskManager {

    /**
     * Concurrent batch-transaction-based mutations buffer:
     *
     * Buffer is structured by tasks. Each task can have multiple transactions, each transaction can have multiple
     * tables and each table can have multiple mutations. Each task is identified by task UUID. Each transaction is
     * identified with transaction UUID. Task sub-buffers are picked up by flusher threads and on success there
     * are two options:
     *
     *      1. the task UUID key is deleted from the the buffer if all transactions are marked for commit.
     *
     *      2. If there is a transactions not marked for commit (large transactions, so buffer is full before
     *         end of transaction is reached), the new task UUID is created and the transaction UUID of the
     *         unfinished transaction is reserved in the new task-sub-buffer.
     *
     * On task failure, task status is updated to 'WRITE_FAILED' and that task will be retried. The hash structure
     * of single task sub-buffer looks like this:
     *
     *  {
     *      "874c3466-3bf0-422f-a3e3-148289226b6c" => { // <- transaction UUID
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
     * Or in short, Perl-like syntax:
     *
     *  $taskBuffer = { $taskUUID => { $transactionUUID => { $tableName => [@Mutations] }}}
     *
     * This works asynchronously for maximum performance. Since transactions are timestamped and they are from RBR
     * we can buffer them in any order. In HBase all of them will be present with corresponding timestamp. And RBR
     * guaranties that each operation is idempotent (so there is no queries that transform data like update value
     * to value * x, which would break the idempotent feature of operations). Simply put, the order of applying of
     * different transactions does not influence the end result since data will be timestamped with timestamps
     * from the binlog and if there are multiple operations on the same row all versions are kept in HBase.
     */
    private final
            ConcurrentHashMap<String, Map<String, Map<String,List<Mutation>>>>
            taskMutationBuffer = new ConcurrentHashMap<String, Map<String, Map<String,List<Mutation>>>>();

    private final
    ConcurrentHashMap<String, Map<String, Map<String,List<String>>>>
            taskRowIDS = new ConcurrentHashMap<String, Map<String, Map<String,List<String>>>>();

    /**
     * Futures grouped by task UUID
     */
    private final
            ConcurrentHashMap<String, Future<Void>>
            taskFutures = new ConcurrentHashMap<String,Future<Void>>();

    /**
     * Status tracking helper structures
     */
    private final ConcurrentHashMap<String, Integer> taskStatus        = new ConcurrentHashMap<String,Integer>();
    private final ConcurrentHashMap<String, Integer> transactionStatus = new ConcurrentHashMap<String,Integer>();

    /**
     * Shared connection used by all tasks in applier
     */
    private Connection hbaseConnection;

    /**
     * Task thread pool
     */
    private static ExecutorService taskPool;

    private final int POOL_SIZE;

    private static volatile String currentTaskUUID;
    private static volatile String currentTransactionUUID;

    public AtomicInteger rowsBufferedCounter = new AtomicInteger(0);

    private final ReplicatorMetrics replicatorMetrics;

    private final Configuration hbaseConf;

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierTaskManager.class);

    // ================================================
    // Constructor
    // ================================================
    public HBaseApplierTaskManager(int poolSize, ReplicatorMetrics repMetrics, org.apache.hadoop.conf.Configuration hbaseConfiguration) {

        POOL_SIZE         = poolSize;
        taskPool          = Executors.newFixedThreadPool(POOL_SIZE);

        hbaseConf         = hbaseConfiguration;
        replicatorMetrics = repMetrics;

        try {
            hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            LOGGER.error("Failed to create hbase connection", e);
        }

        currentTaskUUID = UUID.randomUUID().toString();
        currentTransactionUUID = UUID.randomUUID().toString();

        taskMutationBuffer.put(currentTaskUUID, new HashMap<String, Map<String, List<Mutation>>>());
        taskRowIDS.put(currentTaskUUID, new HashMap<String, Map<String, List<String>>>());

        taskMutationBuffer.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<Mutation>>());
        taskRowIDS.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<String>>());

        taskStatus.put(currentTaskUUID, TaskStatusCatalog.READY_FOR_BUFFERING);
        transactionStatus.put(currentTransactionUUID, TransactionStatus.OPEN);

    }

    // ================================================
    // Buffering util
    // ================================================
    public void pushMutationToTaskBuffer(String tableName, String hbRowKey, Put put) {

        if (taskMutationBuffer.get(currentTaskUUID) == null) {
            LOGGER.error("ERROR: Missing task UUID from taskMutationBuffer keySet. Should never happen. Shutting down...");
            System.exit(1);
        }
        if (taskRowIDS.get(currentTaskUUID) == null) {
            LOGGER.error("ERROR: Missing task UUID from taskRowIDS keySet. Should never happen. Shutting down...");
            System.exit(1);
        }
        if (taskMutationBuffer.get(currentTaskUUID).get(currentTransactionUUID) == null) {
            LOGGER.error("ERROR: Missing transaction UUID from taskMutationBuffer keySet. Should never happen. Shutting down...");
            System.exit(1);
        }
        if (taskRowIDS.get(currentTaskUUID).get(currentTransactionUUID) == null) {
            LOGGER.error("ERROR: Missing transaction UUID from taskRowIDS keySet. Should never happen. Shutting down...");
            for (String tid : taskRowIDS.get(currentTaskUUID).keySet()) {
                LOGGER.info("current task => " + currentTaskUUID + ", current tid => " + currentTransactionUUID + ", available tid => " + tid);
            }
            System.exit(1);
        }

        if (taskMutationBuffer.get(currentTaskUUID).get(currentTransactionUUID).get(tableName) == null) {
            taskMutationBuffer.get(currentTaskUUID).get(currentTransactionUUID).put(tableName, new ArrayList<Mutation>());
            // LOGGER.info("New table in uuid buffer " + uuid + ", initialized key for table " + tableName );
        }
        if (taskRowIDS.get(currentTaskUUID).get(currentTransactionUUID).get(tableName) == null) {
            taskRowIDS.get(currentTaskUUID).get(currentTransactionUUID).put(tableName, new ArrayList<String>());
            // LOGGER.info("New table in uuid buffer " + uuid + ", initialized key for table " + tableName );
        }

        taskMutationBuffer.get(currentTaskUUID).get(currentTransactionUUID).get(tableName).add(put);
        taskRowIDS.get(currentTaskUUID).get(currentTransactionUUID).get(tableName).add(hbRowKey);

        rowsBufferedCounter.incrementAndGet();
    }

    // ================================================
    // Flushing util
    // ================================================
    public void markCurrentTransactionForCommit() {

        // mark
        transactionStatus.put(currentTransactionUUID, TransactionStatus.READY_FOR_COMMIT);

        // open a new transaction slot and set it as the current transaction
        currentTransactionUUID = UUID.randomUUID().toString();
        taskMutationBuffer.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<Mutation>>());
        taskRowIDS.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<String>>());
        transactionStatus.put(currentTransactionUUID, TransactionStatus.OPEN);
    }

    public void flushCurrentTaskBuffer() {

        // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
        markCurrentTaskAsReadyAndCreateNewUUIDBuffer(); // <- this one is blocking
        flushBufferedTask(); // <- current and those which previously failed
    }

    private void markCurrentTaskAsReadyAndCreateNewUUIDBuffer() {

        // don't create new buffers if no slots available
        blockIfNoSlotsAvailableForBuffering();

        // mark current uuid buffer as READY_FOR_PICK_UP
        taskStatus.put(currentTaskUUID, TaskStatusCatalog.READY_FOR_PICK_UP);

        // create new uuid buffer
        String newTaskUUID = UUID.randomUUID().toString();

        taskMutationBuffer.put(newTaskUUID, new HashMap<String, Map<String, List<Mutation>>>());
        taskRowIDS.put(newTaskUUID, new HashMap<String, Map<String, List<String>>>());

        // Check if there is an open/unfinished transaction in current UUID task buffer and
        // if so, create/reserve the corresponding transaction UUID in the new UUID task buffer
        // so that the transaction rows that are on the way can be buffered under the same UUID.
        // This is a foundation for the TODO: when XID event is received and the end of transaction tie
        // the transaction id from XID with the transaction UUID used for buffering. The goal is
        // to be able to identify mutations in HBase which were part of the same transaction.
        int openTransactions = 0;
        for (String transactionUUID : taskMutationBuffer.get(currentTaskUUID).keySet()) {
            if (transactionStatus.get(transactionUUID) == TransactionStatus.OPEN) {
                openTransactions++;
                if (openTransactions > 1) {
                    LOGGER.error("More than one partial transaction in the buffer. Should never happen! Exiting...");
                    System.exit(-1);
                }
                taskMutationBuffer.get(newTaskUUID).put(transactionUUID, new HashMap<String,List<Mutation>>() );
                taskRowIDS.get(newTaskUUID).put(transactionUUID, new HashMap<String,List<String>>() );
                currentTransactionUUID = transactionUUID; // <- important
            }
        }

        taskStatus.put(newTaskUUID, TaskStatusCatalog.READY_FOR_BUFFERING);

        currentTaskUUID = newTaskUUID;

        // update task queue size
        long taskQueueSize = 0;
        for (String taskUUID : taskStatus.keySet()) {
            if (taskStatus.get(taskUUID) == TaskStatusCatalog.READY_FOR_PICK_UP) {
                taskQueueSize++;
            }
        }
        replicatorMetrics.setTaskQueueSize(taskQueueSize);
    }

    private void blockIfNoSlotsAvailableForBuffering() {

        boolean block = true;
        int blockingTime = 0;

        while (block) {

            updateTaskStatuses();

            int currentNumberOfTasks = taskMutationBuffer.keySet().size();

            if (currentNumberOfTasks > POOL_SIZE) {
                try {
                    Thread.sleep(10);
                    blockingTime += 10;
                }
                catch (InterruptedException e) {
                    LOGGER.error("Cant sleep.", e);
                }
                if ((blockingTime % 1000) == 0) {
                    LOGGER.warn("To many tasks already open ( " + currentNumberOfTasks + " ), blocking time is " + blockingTime + "ms");
                }
            }
            else {
                block = false;
            }
        }
    }

    private void updateTaskStatuses() {

        // clean up and re-queue failed tasks
        Set<String> taskFuturesUUIDs = taskFutures.keySet();

        for (String submitedTaskUUID : taskFuturesUUIDs) {

            Future<Void> taskFuture = taskFutures.get(submitedTaskUUID);

            if (taskFuture.isDone()) {

                if (taskStatus.get(submitedTaskUUID) == TaskStatusCatalog.WRITE_SUCCEEDED) {

                    taskStatus.remove(submitedTaskUUID);

                    taskMutationBuffer.remove(submitedTaskUUID); // <- if there are open transaction UUID in this task, they have been copied to new task
                    taskRowIDS.remove(submitedTaskUUID); // <== --||--

                    // since the task is done, remove the key from the futures hash
                    taskFutures.remove(submitedTaskUUID);
                } else if (taskStatus.get(submitedTaskUUID) == TaskStatusCatalog.WRITE_FAILED) {

                    LOGGER.warn("Task " + submitedTaskUUID +
                            " failed with status => " + taskStatus.get(submitedTaskUUID) +
                            ". Task will be retried."
                    );

                    // remove the key from the futures hash; new future will be created
                    taskFutures.remove(submitedTaskUUID);

                    // keep the mutation buffer, just change the status so this task is picked up again
                    taskStatus.put(submitedTaskUUID, TaskStatusCatalog.READY_FOR_PICK_UP);
                } else {
                    LOGGER.error("Task status error for STATUS => " + taskStatus.get(submitedTaskUUID).toString());
                }
            }
        }
    }

    /**
     * Submit tasks that are READY_FOR_PICK_UP
     */
    private void flushBufferedTask() {

        if (hbaseConnection == null) {
            LOGGER.info("HBase connection is gone. Will try to recreate new connection...");
            int retry = 10;
            while (retry > 0) {
                try {
                    hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
                    retry = 0;
                } catch (IOException e) {
                    LOGGER.warn("Failed to create hbase connection from HBaseApplier, attempt " + retry + "/10");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.error("Thread wont sleep. Not a good day for you.",e);
                }
                retry--;
            }
        }

        if (hbaseConnection == null) {
            LOGGER.error("Could not create HBase connection, all retry attempts failed. Exiting...");
            System.exit(-1);
        }

        // one future per task
        for (final String taskUUID : taskStatus.keySet()) {

            if (taskStatus.get(taskUUID) == TaskStatusCatalog.READY_FOR_PICK_UP) {

                taskStatus.put(taskUUID,TaskStatusCatalog.TASK_SUBMITTED);
                replicatorMetrics.incApplierTasksSubmittedCounter();

                taskFutures.put(taskUUID, taskPool.submit(new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {

                        try {

                            int numberOfTransactionsInTask = taskMutationBuffer.get(taskUUID).keySet().size();
                            long numberOfRowsInTask = 0;
                            for (String transactionUUID : taskRowIDS.get(taskUUID).keySet()) {
                                for (String tableName : taskRowIDS.get(taskUUID).get(transactionUUID).keySet()) {
                                    List<String> bufferedIDs = taskRowIDS.get(taskUUID).get(transactionUUID).get(tableName);
                                    int numberOfBufferedIDsForTable = bufferedIDs.size();
                                    numberOfRowsInTask += numberOfBufferedIDsForTable;
                                }
                            }

                            taskStatus.put(taskUUID, TaskStatusCatalog.WRITE_IN_PROGRESS);
                            replicatorMetrics.incApplierTasksInProgressCounter();

                            for (final String transactionUUID : taskMutationBuffer.get(taskUUID).keySet()) {

                                int numberOfTablesInCurrentTransaction = taskMutationBuffer.get(taskUUID).get(transactionUUID).keySet().size();

                                int numberOfFlushedTablesInCurrentTransaction = 0;

                                for (final String bufferedTableName : taskMutationBuffer.get(taskUUID).get(transactionUUID).keySet()) {

                                    // One mutator and listener for each table in transaction (good for big transactions)
                                    TableName TABLE = TableName.valueOf(bufferedTableName);

                                    // create listener
                                    BufferedMutator.ExceptionListener listener =
                                            new BufferedMutator.ExceptionListener() {
                                                @Override
                                                public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                                                    // This is callback is executed in mutator.flush()
                                                    for (int i = 0; i < e.getNumExceptions(); i++) {
                                                        LOGGER.error("Failed to send put to table " + bufferedTableName + e.getRow(i));
                                                    }
                                                    taskStatus.put(taskUUID, TaskStatusCatalog.WRITE_FAILED);
                                                }
                                            };

                                    // attach listener
                                    BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

                                    // get mutator
                                    BufferedMutator mutator = hbaseConnection.getBufferedMutator(params);

                                    mutator.mutate(taskMutationBuffer.get(taskUUID).get(transactionUUID).get(bufferedTableName)); // <<- List<Mutation> for the given table
                                    mutator.flush(); // <- flush per table, but fork per batch (which can have many tables)
                                    mutator.close();

                                    numberOfFlushedTablesInCurrentTransaction++;

                                } // next table

                                // callback report
                                if (taskStatus.get(taskUUID) == TaskStatusCatalog.WRITE_FAILED) {
                                    throw new IOException(
                                            "RetriesExhaustedWithDetailsException caught. Task " +
                                            taskUUID + " failed."
                                    );
                                }

                                if (numberOfTablesInCurrentTransaction != numberOfFlushedTablesInCurrentTransaction) {
                                    throw new IOException("Failed to write all tables in the transaction "
                                            + transactionUUID
                                            + ". Number of present tables => "
                                            + numberOfTablesInCurrentTransaction
                                            + ". Number of flushed tables => "
                                            + numberOfFlushedTablesInCurrentTransaction
                                    );
                                }
                            } // next transaction

                            taskStatus.put(taskUUID, TaskStatusCatalog.WRITE_SUCCEEDED);

                            replicatorMetrics.incApplierTasksSucceededCounter();

                            replicatorMetrics.deltaIncRowOpsSuccessfullyCommited(numberOfRowsInTask);

                            //long totalRowsSoFar = replicatorMetrics.getTotalRowsSuccessfullyInserted();
                            replicatorMetrics.incTotalRowOpsSuccessfullyCommited(numberOfRowsInTask);

                        } catch (IOException e) {
                            LOGGER.error(
                                    "Failed to flush buffer for transaction " +
                                    taskUUID +
                                    ". Marking this task for retry...", e
                            );
                            // if one transaction fails, the whole task is retried.
                            taskStatus.put(taskUUID, TaskStatusCatalog.WRITE_FAILED);
                            replicatorMetrics.incApplierTasksFailedCounter();
                        }
                        return null;
                    }
                }));
            }
        }
    }
}
