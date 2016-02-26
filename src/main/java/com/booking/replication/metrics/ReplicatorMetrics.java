package com.booking.replication.metrics;

import com.booking.replication.util.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by bosko on 1/19/16.
 */
public class ReplicatorMetrics {

    private final ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> replicatorMetrics;

    private final ConcurrentHashMap<Integer, MutableLong> totals;

    private final AtomicBoolean bucketInitializationInProgess = new AtomicBoolean(false);

    private final Integer beginTime;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicatorMetrics.class);

    public ReplicatorMetrics() {
        beginTime = (int) (System.currentTimeMillis() / 1000L);
        replicatorMetrics = new  ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>>();
        totals = new ConcurrentHashMap<Integer, MutableLong>();
        initTotals();
    }

    private void initTotals() {
        initTotal(Metric.TOTAL_EVENTS_RECEIVED);
        initTotal(Metric.TOTAL_EVENTS_SKIPPED);
        initTotal(Metric.TOTAL_EVENTS_PROCESSED);

        initTotal(Metric.TOTAL_INSERT_EVENTS_COUNTER);
        initTotal(Metric.TOTAL_UPDATE_EVENTS_COUNTER);
        initTotal(Metric.TOTAL_DELETE_EVENTS_COUNTER);
        initTotal(Metric.TOTAL_COMMIT_COUNTER);
        initTotal(Metric.TOTAL_XID_COUNTER);

        initTotal(Metric.TOTAL_ROWS_PROCESSED);
        initTotal(Metric.TOTAL_ROWS_FOR_INSERT_PROCESSED);
        initTotal(Metric.TOTAL_ROWS_FOR_UPDATE_PROCESSED);
        initTotal(Metric.TOTAL_ROWS_FOR_DELETE_PROCESSED);

        initTotal(Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED);

        initTotal(Metric.TOTAL_HEART_BEAT_COUNTER);

        initTotal(Metric.TOTAL_APPLIER_TASKS_SUBMITTED);
        initTotal(Metric.TOTAL_APPLIER_TASKS_IN_PROGRESS);
        initTotal(Metric.TOTAL_APPLIER_TASKS_SUCCEEDED);
        initTotal(Metric.TOTAL_APPLIER_TASKS_FAILED);
    }

    private void initTotal(Integer totalID) {
        totals.put(totalID, new MutableLong());
    }

    public void initTimebucket(int currentTimeSeconds) {

        // this can be called by multiple threads for the same bucket, so some
        // protection is needed to make sure that bucket is initialized only once
        if (bucketInitializationInProgess.compareAndSet(false,true)) {

            this.replicatorMetrics.put(currentTimeSeconds, new HashMap<Integer, MutableLong>());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.EVENTS_RECEIVED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.EVENTS_SKIPPED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.EVENTS_PROCESSED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.INSERT_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.UPDATE_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.DELETE_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.COMMIT_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.XID_COUNTER, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_PROCESSED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_FOR_INSERT_PROCESSED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_FOR_UPDATE_PROCESSED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_FOR_DELETE_PROCESSED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.HEART_BEAT_COUNTER, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.APPLIER_TASKS_SUBMITTED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.APPLIER_TASKS_IN_PROGRESS, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.APPLIER_TASKS_SUCCEEDED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.APPLIER_TASKS_FAILED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.REPLICATION_DELAY_MS, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROW_OPS_SUCCESSFULLY_COMMITED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.TASK_QUEUE_SIZE, new MutableLong());

            // TOTALS
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_EVENTS_RECEIVED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_EVENTS_SKIPPED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_EVENTS_PROCESSED);

            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_INSERT_EVENTS_COUNTER);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_UPDATE_EVENTS_COUNTER);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_DELETE_EVENTS_COUNTER);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_COMMIT_COUNTER);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_XID_COUNTER);

            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_ROWS_PROCESSED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_ROWS_FOR_INSERT_PROCESSED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_ROWS_FOR_UPDATE_PROCESSED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_ROWS_FOR_DELETE_PROCESSED);

            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED);

            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_HEART_BEAT_COUNTER);

            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_SUBMITTED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_IN_PROGRESS);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_SUCCEEDED);
            initTimebucketForTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_FAILED);

            // release the lock
            bucketInitializationInProgess.compareAndSet(true,false);
        }
        else {
            // bucket initialization in progress by another thread. Wait until
            // lock is released
            while (bucketInitializationInProgess.get() == true);
        }
    }

    public ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> getMetrics() {
        return replicatorMetrics;
    }

    // ROWS
    public void incRowsInsertedCounter  () {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.ROWS_FOR_INSERT_PROCESSED);
        incTotal(currentTimeSeconds, Metric.TOTAL_ROWS_FOR_INSERT_PROCESSED, 1L);
    }

    public void incRowsUpdatedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.ROWS_FOR_UPDATE_PROCESSED);
        incTotal(currentTimeSeconds, Metric.TOTAL_ROWS_FOR_UPDATE_PROCESSED, 1L);
    }

    public void incRowsDeletedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.ROWS_FOR_DELETE_PROCESSED);
        incTotal(currentTimeSeconds, Metric.TOTAL_ROWS_FOR_DELETE_PROCESSED, 1L);
    }

    public void incRowsProcessedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.ROWS_PROCESSED);
        incTotal(currentTimeSeconds, Metric.TOTAL_ROWS_PROCESSED, 1L);
    }


    // EVENTS
    public void incInsertEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.INSERT_EVENTS_COUNTER);
        incTotal(currentTimeSeconds, Metric.TOTAL_INSERT_EVENTS_COUNTER, 1L);
    }

    public void incUpdateEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.UPDATE_EVENTS_COUNTER);
        incTotal(currentTimeSeconds, Metric.TOTAL_UPDATE_EVENTS_COUNTER, 1L);
    }

    public void incDeleteEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.DELETE_EVENTS_COUNTER);
        incTotal(currentTimeSeconds, Metric.TOTAL_DELETE_EVENTS_COUNTER, 1L);
    }

    public void incCommitQueryCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.COMMIT_COUNTER);
        incTotal(currentTimeSeconds, Metric.TOTAL_COMMIT_COUNTER, 1L);
    }

    public void incXIDCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.XID_COUNTER);
        incTotal(currentTimeSeconds, Metric.TOTAL_XID_COUNTER, 1L);
    }

    public void incEventsReceivedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.EVENTS_RECEIVED);
        incTotal(currentTimeSeconds, Metric.TOTAL_EVENTS_RECEIVED, 1L);
    }

    public void incEventsProcessedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.EVENTS_PROCESSED);
        incTotal(currentTimeSeconds, Metric.TOTAL_EVENTS_PROCESSED, 1L);
    }

    public void incEventsSkippedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.EVENTS_SKIPPED);
        incTotal(currentTimeSeconds, Metric.TOTAL_EVENTS_SKIPPED, 1L);
    }

    public void incHeartBeatCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.HEART_BEAT_COUNTER);
        incTotal(currentTimeSeconds, Metric.TOTAL_HEART_BEAT_COUNTER, 1L);
    }

    public void incApplierTasksSubmittedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.APPLIER_TASKS_SUBMITTED);
        incTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_SUBMITTED, 1L);
    }

    public void incApplierTasksSucceededCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.APPLIER_TASKS_SUCCEEDED);
        incTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_SUCCEEDED, 1L);
    }

    public void incApplierTasksFailedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.APPLIER_TASKS_FAILED);
        incTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_FAILED, 1L);
    }

    public void incApplierTasksInProgressCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incCounter(currentTimeSeconds, Metric.APPLIER_TASKS_IN_PROGRESS);
        incTotal(currentTimeSeconds, Metric.TOTAL_APPLIER_TASKS_IN_PROGRESS, 1L);
    }

    // set
    // TODO: add separate metrics for commitedToHBaseReplicationDelay
    public void setReplicatorReplicationDelay(Long replicatorReplicationDelay) {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucket(currentTimeSeconds);
        }
        if (this.replicatorMetrics.get(currentTimeSeconds).get(Metric.REPLICATION_DELAY_MS) != null) {
            this.replicatorMetrics.get(currentTimeSeconds).get(Metric.REPLICATION_DELAY_MS).setValue(replicatorReplicationDelay);
        }
        else {
            LOGGER.warn("Failed to properly initialize timebucket " + currentTimeSeconds);
        }
    }

    // delta inc
    public void deltaIncRowOpsSuccessfullyCommited(Long delta) {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucket(currentTimeSeconds);
        }
        if (this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROW_OPS_SUCCESSFULLY_COMMITED) != null) {
            long oldValue = this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROW_OPS_SUCCESSFULLY_COMMITED).getValue();
            long newValue = oldValue + delta;
            this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROW_OPS_SUCCESSFULLY_COMMITED).setValue(newValue);
        }
        else {
            LOGGER.warn("Failed to properly initialize timebucket " + currentTimeSeconds);
        }
    }

    public void incTotalRowOpsSuccessfullyCommited(Long delta) {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        incTotal(currentTimeSeconds, Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED, delta);
    }

    public void initTimebucketForTotal(int currentTimeSeconds, int totalID) {
        long currentTotalValue = totals.get(totalID).getValue();
        this.replicatorMetrics.get(currentTimeSeconds).put(totalID, new MutableLong(currentTotalValue));
    }

    public void setTaskQueueSize(Long newValue) {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucket(currentTimeSeconds);
        }
        if (this.replicatorMetrics.get(currentTimeSeconds).get(Metric.TASK_QUEUE_SIZE) != null) {
            this.replicatorMetrics.get(currentTimeSeconds).get(Metric.TASK_QUEUE_SIZE).setValue(newValue);
        }
        else {
            LOGGER.warn("Failed to properly initialize timebucket " + currentTimeSeconds);
        }
    }
    // Util
    private boolean noNulls(Integer timebucket, Integer metricID) {

        if (this.replicatorMetrics.get(timebucket) == null) {
            return false;
        }
        if (this.replicatorMetrics.get(timebucket).get(metricID) == null) {
            return false;
        }
        return true;
    }

    private void incCounter(Integer timebucket, Integer counterID) {

        while (bucketInitializationInProgess.get() == true);

        try {
            if (noNulls(timebucket, counterID)) {
                this.replicatorMetrics.get(timebucket).get(counterID).increment();
                return;
            } else {
                if (bucketInitializationInProgess.get() == false) {
                    initTimebucket(timebucket);
                }
            }

            if (noNulls(timebucket, counterID)) {
                this.replicatorMetrics.get(timebucket).get(counterID).increment();
                return;
            } else {
                LOGGER.warn("Failed to properly initialize timebucket " + timebucket + " for metricID " + counterID + ". This should not happen - needs fixing!");
            }
        }
        catch (NullPointerException e) {
            LOGGER.warn("NullPointerException checks are not good enough. Needs fixing!", e);
        }
    }

    public void incTotal(Integer timebucket, Integer totalID, Long delta) {

        while (bucketInitializationInProgess.get() == true);

        try {
            if (noNulls(timebucket, totalID)) {
                deltaIncTotal(timebucket,totalID, delta);
                return;
            } else {
                if (bucketInitializationInProgess.get() == false) {
                    initTimebucket(timebucket);
                }
            }

            if (noNulls(timebucket, totalID)) {
                deltaIncTotal(timebucket, totalID, delta);
                return;
            } else {
                LOGGER.warn("Failed to properly initialize timebucket " + timebucket + " for metricID " + totalID + ". This should not happen - needs fixing!");
            }
        }
        catch (NullPointerException e) {
            LOGGER.warn("NullPointerException checks are not good enough. Needs fixing!", e);
        }
    }

    private void deltaIncTotal(Integer currentTimeSeconds, Integer metricID, Long delta) {
        if (this.replicatorMetrics.get(currentTimeSeconds).get(metricID) != null) {
            synchronized (this) {
                long oldValue = totals.get(metricID).getValue();
                long newValue = oldValue + delta;
                totals.put(metricID, new MutableLong(newValue));
                replicatorMetrics.get(currentTimeSeconds).get(metricID).setValue(newValue);
            }
        }
        else {
            LOGGER.warn("Failed to properly initialize timebucket " + currentTimeSeconds);
        }
    }
}
