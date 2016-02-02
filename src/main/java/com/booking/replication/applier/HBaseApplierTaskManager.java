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
import java.util.concurrent.atomic.AtomicLong;

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
    private final ConcurrentHashMap<String, Map<String, Map<String,List<Mutation>>>> tasksBuffer = new ConcurrentHashMap<String, Map<String, Map<String,List<Mutation>>>>();

    /**
     * Status tracking helper structures
     */
    private final ConcurrentHashMap<String, Integer> taskStatus = new ConcurrentHashMap<String,Integer>();
    private final ConcurrentHashMap<String, Integer> transactionStatus = new ConcurrentHashMap<String,Integer>();

    /**
     * Helper buffers
     */
    private final ConcurrentHashMap<String, Future<Integer>> submittedTasks = new ConcurrentHashMap<String,Future<Integer>>();
    private final ConcurrentHashMap<String, Integer>         doneTasks      = new ConcurrentHashMap<String, Integer>();

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

    // TODO: store these counters as graphite metrics
    public AtomicLong    transactionsWritten = new AtomicLong(0);
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

        tasksBuffer.put(currentTaskUUID, new HashMap<String, Map<String, List<Mutation>>>());
        tasksBuffer.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<Mutation>>());

        taskStatus.put(currentTaskUUID, TaskStatus.READY_FOR_BUFFERING);

        transactionStatus.put(currentTransactionUUID, TransactionStatus.OPEN);

    }

    // ================================================
    // Buffering util
    // ================================================
    public void pushMutationToTaskBuffer(String tableName, Put put) {

        if (tasksBuffer.get(currentTaskUUID) == null) {
            LOGGER.error("ERROR: Missing task UUID from tasksBuffer keySet. Should never happen. Shutting down...");
            System.exit(1);
        }
        if (tasksBuffer.get(currentTaskUUID).get(currentTransactionUUID) == null) {
            LOGGER.error("ERROR: Missing transaction UUID from tasksBuffer keySet. Should never happen. Shutting down...");
            System.exit(1);
        }
        if (tasksBuffer.get(currentTaskUUID).get(currentTransactionUUID).get(tableName) == null) {
            tasksBuffer.get(currentTaskUUID).get(currentTransactionUUID).put(tableName, new ArrayList<Mutation>());
            // LOGGER.info("New table in uuid buffer " + uuid + ", initialized key for table " + tableName );
        }
        tasksBuffer.get(currentTaskUUID).get(currentTransactionUUID).get(tableName).add(put);

        rowsBufferedCounter.incrementAndGet();
    }

    // ================================================
    // Flushing util
    // ================================================
    public void markCurrentTransactionForCommit() {

        // mark
        transactionStatus.put(currentTransactionUUID, TransactionStatus.READY_FOR_COMMIT);

        // set new transaction as current in the current task
        currentTransactionUUID = UUID.randomUUID().toString();
        tasksBuffer.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<Mutation>>());
        transactionStatus.put(currentTransactionUUID, TransactionStatus.OPEN);
    }

    public void flushCurrentTaskBuffer() {

        // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
        markCurrentAsReadyAndCreateNewUUIDBuffer(); // <- this one is blocking
        flushBufferedTask(); // <- current and those which previously failed
    }

    private void markCurrentAsReadyAndCreateNewUUIDBuffer() {

        // don't create new buffers if no slots available
        blockIfNoSlotsAvailableForBuffering();

        // mark current uuid buffer as READY_FOR_PICK_UP
        taskStatus.put(currentTaskUUID, TaskStatus.READY_FOR_PICK_UP);

        // create new uuid buffer
        String newTaskUUID = UUID.randomUUID().toString();

        tasksBuffer.put(newTaskUUID, new HashMap<String, Map<String, List<Mutation>>>());

        // Check if there is an open/unfinished transaction in current UUID task buffer and
        // if so, create/reserve the corresponding transaction UUID in the new UUID task buffer
        // so that the transaction rows that are on the way can be buffered under the same UUID.
        // This is part of the TODO: when XID event is received and the end of transaction tie
        // the transaction id from XID with the transaction UUID used for buffering. The goal is
        // to be able to identify mutations in HBase which were part of the same transaction.
        for (String transactionUUID : tasksBuffer.get(currentTaskUUID).keySet()) {
            if (transactionStatus.get(transactionUUID) == TransactionStatus.OPEN) {
                tasksBuffer.get(newTaskUUID).put(transactionUUID, new HashMap<String,List<Mutation>>() );
            }
        }

        taskStatus.put(newTaskUUID, TaskStatus.READY_FOR_BUFFERING);

        currentTaskUUID = newTaskUUID;
    }

    private void blockIfNoSlotsAvailableForBuffering() {

        boolean block = true;
        while (block) {

            updateTaskStatuses();

            int currentNumberOfTasks = tasksBuffer.keySet().size();

            if (currentNumberOfTasks > POOL_SIZE) {
                LOGGER.warn("To many tasks already open ( " + currentNumberOfTasks + " ), blocking further writes for 1s...");
                LOGGER.info("Number of written transactions so far " + transactionsWritten.get());
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    LOGGER.error("Cant sleep.", e);
                }
            }
            else {
                block = false;
            }
        }
    }

    /**
     * Check the number of running tasks. If number < max number then we can spawn more.
     * If number = max, then wait for at least one task to finish and only then give green
     * light to continue (which means: do the clean up and create new transaction buffer slot)
     * @return
     */
    private void updateTaskStatuses() {

        // 1. update doneTasks
        for (String submittedTaskUUID : submittedTasks.keySet()) {
            Future<Integer> task = submittedTasks.get(submittedTaskUUID);
            try {
                if (task.isDone()) {

                    Integer result = task.get();

                    if (result != null) {
                        if (result == TaskStatus.WRITE_SUCCEEDED) {

                            // uuid buffer flushed, mark buffer with WRITE_SUCCEEDED
                            doneTasks.put(submittedTaskUUID, TaskStatus.WRITE_SUCCEEDED);
                            // LOGGER.info("+++WRITE_SUCCEEDED+++ Acknowledged by Applier!");

                        } else if (result == TaskStatus.WRITE_FAILED) {

                            //  mark buffer with WRITE_FAILED
                            LOGGER.warn("===WRITE_FAILED=== Acknowledged by Applier!");
                            doneTasks.put(submittedTaskUUID, TaskStatus.WRITE_FAILED);

                        } else {
                            LOGGER.error("vvv===UNKNOWN_STATUS===vvv [" + result + "] for transaction buffer " + submittedTaskUUID);
                        }
                    } else {
                        LOGGER.error("Received null result from task for transaction buffer " + submittedTaskUUID);
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("Could not get result from the fut ure. Interrupted exception", e);
            } catch (ExecutionException e) {
                Throwable te = e.getCause();
                LOGGER.warn("Future task failed with exception => " + te.getMessage() + " " + te.getStackTrace());
                LOGGER.info("Will re-queue the task.");
                doneTasks.put(submittedTaskUUID, TaskStatus.WRITE_FAILED);
            }
        }

        // 2. clean up and re-queue failed tasks
        for (String taskUUID : doneTasks.keySet()) {
            if (doneTasks.get(taskUUID) == TaskStatus.WRITE_SUCCEEDED) {
                replicatorMetrics.incApplierTasksSucceededCounter();
                taskStatus.remove(taskUUID);
                tasksBuffer.remove(taskUUID); // <- if there are open transaction UUID in this task, they have been copied to new task
                // LOGGER.info("Successful task " + taskUUID + " removed from the hbaseApplierTaskManager pool");
            }
            else {
                // keep the buffer, just change the status so this task is picked up again
                LOGGER.warn("Task " + taskUUID + " failed with status => " + doneTasks.get(taskUUID) + ". Will re-queue");
                taskStatus.put(taskUUID, TaskStatus.READY_FOR_PICK_UP);
            }
            submittedTasks.remove(taskUUID);
        }
    }

    /**
     * Commit transactions that are fully buffered
     */
    private void flushBufferedTask() {

        //LOGGER.info("Submitting buffered transactions...");

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

        // one future per task
        for (final String taskUUID : taskStatus.keySet()) {

            if (taskStatus.get(taskUUID) == TaskStatus.READY_FOR_PICK_UP) {

                submittedTasks.put(taskUUID, taskPool.submit(new Callable<Integer>() {

                    @Override
                    public Integer call() throws Exception {

                        int numberOfTransactionsInTask = tasksBuffer.get(taskUUID).keySet().size();

                        try {

                            for (final String transactionUUID : tasksBuffer.get(taskUUID).keySet()) {

                                int numberOfTablesInCurrentTransaction = tasksBuffer.get(taskUUID).get(transactionUUID).keySet().size();

                                int numberOfFlushedTablesInCurrentTransaction = 0;

                                for (final String bufferedTableName : tasksBuffer.get(taskUUID).get(transactionUUID).keySet()) {

                                    // One mutator and listener for each table in transaction (good for big transactions)
                                    TableName TABLE = TableName.valueOf(bufferedTableName);

                                    // create listener
                                    BufferedMutator.ExceptionListener listener =
                                            new BufferedMutator.ExceptionListener() {
                                                @Override
                                                public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                                                    for (int i = 0; i < e.getNumExceptions(); i++) {
                                                        LOGGER.warn("Failed to send put to table " + bufferedTableName + e.getRow(i));
                                                    }
                                                }
                                            };

                                    // attach listener
                                    BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

                                    // getValue mutator
                                    BufferedMutator mutator = hbaseConnection.getBufferedMutator(TABLE);

                                    taskStatus.put(taskUUID, TaskStatus.WRITE_IN_PROGRESS);

                                    mutator.mutate(tasksBuffer.get(taskUUID).get(transactionUUID).get(bufferedTableName)); // <<- List<Mutation> for the given table
                                    mutator.flush(); // <- flush per table, but fork per batch (which can have many tables)
                                    mutator.close();

                                    numberOfFlushedTablesInCurrentTransaction++;

                                } // next table

                                if (numberOfTablesInCurrentTransaction == numberOfFlushedTablesInCurrentTransaction) {
                                    transactionsWritten.incrementAndGet();
                                }
                                else {
                                    throw new IOException("Failed to write all tables in the transaction "
                                            + transactionUUID
                                            + ". Number of present tables => "
                                            + numberOfTablesInCurrentTransaction
                                            + ". Number of flushed tables => "
                                            + numberOfFlushedTablesInCurrentTransaction
                                    );
                                }

                            } // next transaction

                        } catch (IOException e) {
                            LOGGER.error("Failed to flush buffer for transaction " + taskUUID + ". Marking this task for retry...", e);

                            // TODO: not optimal to retry the whole task. Only failed transactions can be marked for retry
                            taskStatus.put(taskUUID, TaskStatus.WRITE_FAILED);
                            return TaskStatus.WRITE_FAILED;
                        }

                        return TaskStatus.WRITE_SUCCEEDED;
                    }
                }));
            }
        }
    }
}
