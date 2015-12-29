package com.booking.replication.pipeline;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.STDOUTJSONApplier;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.schema.HBaseSchemaManager;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.applier.HBaseApplier;
import com.booking.replication.stats.Counters;
import com.booking.replication.util.MutableLong;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PipelineOrchestrator
 *
 * On each event handles:
 *      1. schema version management
 *      2  augmenting events with schema info
 *      3. sending of events to applier.
 */
public class PipelineOrchestrator extends Thread {

    private static EventAugmenter eventAugmenter;

    private static HBaseSchemaManager hBaseSchemaManager;

    private final LinkedBlockingQueue<BinlogEventV4> queue;

    private final ConcurrentHashMap<Integer,Object> lastKnownInfo;

    private final ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> pipelineStats;

    public CurrentTransactionMetadata currentTransactionMetadata;

    private boolean running = false;
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineOrchestrator.class);

    public final Configuration configuration;
    private final Applier applier;

    public long consumerStatsNumberOfProcessedRows = 0;
    public long consumerStatsNumberOfProcessedEvents = 0;

    public long consumerTimeM1 = 0;
    public long consumerTimeM1_WriteV2 = 0;

    public long consumerTimeM2 = 0;

    public long consumerTimeM3 = 0;

    public long consumerTimeM4 = 0;

    public long consumerTimeM5 = 0;

    public long eventCounter = 0;


   /**
    * fakeMicrosecondCounter: this is a special feature that
    * requires some explanation
    *
    * MySQL binlog events have second-precision timestamps. This
    * obviously means that we can't have microsecond precision,
    * but that is not the intention here. The idea is to at least
    * preserve the information about ordering of events,
    * especially if one ID has multiple events within the same
    * second. We want to know what was their order. That is the
    * main purpose of this counter.
    */
    private static long fakeMicrosecondCounter = 0;

    public static void setFakeMicrosecondCounter(Long fakeMicrosecondCounter) {
        PipelineOrchestrator.fakeMicrosecondCounter = fakeMicrosecondCounter;
    }

    public PipelineOrchestrator(
            LinkedBlockingQueue<BinlogEventV4> q,
            ConcurrentHashMap<Integer,Object> chm,
            Configuration repcfg,
            ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> pStats
        ) throws SQLException, URISyntaxException, IOException {

        this.pipelineStats = pStats;

        this.configuration = repcfg;

        eventAugmenter = new EventAugmenter(repcfg);

        currentTransactionMetadata = new CurrentTransactionMetadata();

        if (configuration.getApplierType().equals("hbase")) {
            hBaseSchemaManager = new HBaseSchemaManager(repcfg.getZOOKEEPER_QUORUM());
        }

        queue = q;

        if (repcfg.getApplierType().equals("STDOUT")) {
            applier = new STDOUTJSONApplier();
        }
        else if (repcfg.getApplierType().toLowerCase().equals("hbase")) {
            applier = new HBaseApplier(repcfg.getZOOKEEPER_QUORUM());
        }
        else {
            LOGGER.warn("Unknown applier type. Defaulting to STDOUT");
            applier = new STDOUTJSONApplier();
        }

        lastKnownInfo = chm;

        LOGGER.info("Created consumer with lastKnownInfo position => { "
                + " binlogFileName => "
                +   ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION)).getBinlogFilename()
                + ", binlogPosition => "
                +   ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION)).getBinlogPosition()
                + " }"
        );
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void stopRunning() {
        setRunning(false);
    }

    @Override
    public void run() {

        setRunning(true);

        while (isRunning()) {
            try {
                if (queue.size() > 0) {

                    BinlogEventV4 event =
                            queue.poll(100, TimeUnit.MILLISECONDS);

                    if (event != null) {
                        eventCounter++;
                        if (!skipEvent(event)) {
                            calculateAndPropagateChanges(event);
                        }
                        if (eventCounter % 1000 == 0) {
                            LOGGER.info("Consumer reporting queue size => " + queue.size());
                        }
                    } else {
                        LOGGER.error("Poll timeout. Will sleep for 1s and try again.");
                        Thread.sleep(1000);
                    }
                }
                else {
                    LOGGER.info("Consumer reporting: no items in event queue. Will sleep for 5s and check again.");
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TableMapException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  calculateAndPropagateChanges
     *
     *     STEPS:
     *     ======
     *  1. check event type
     *
     *  2. if DDL:
     *      a. pass to eventAugmenter which will update the schema
     *      b. calculateAndPropagateChanges event to HBase
     *
     *  3. if DATA:
     *      a. match column names and types
     *      b. calculateAndPropagateChanges event to HBase
     */
    public void calculateAndPropagateChanges(BinlogEventV4 event) throws IOException, TableMapException {

        AugmentedRowsEvent augmentedRowsEvent;
        AugmentedSchemaChangeEvent augmentedSchemaChangeEvent;

        if (fakeMicrosecondCounter > 999998) {
            fakeMicrosecondCounter = 0;
        }

        checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists();

        long tStart;
        long tEnd;
        long tDelta;

        switch (event.getHeader().getEventType()) {

            // DDL Event:
            case MySQLConstants.QUERY_EVENT:
                fakeMicrosecondCounter++;
                incCommitQueryCounter();
                injectFakeMicroSecondsIntoEventTimestamp(event);
                String querySQL = ((QueryEvent) event).getSql().toString();
                if (isCOMMIT(querySQL)) {
                    applier.applyCommitQueryEvent((QueryEvent) event, this);
                }
                else if (isDDL(querySQL)) {
                    long tStart5 = System.currentTimeMillis();
                    augmentedSchemaChangeEvent = eventAugmenter.transitionSchemaToNextVersion(event);
                    long tEnd5 = System.currentTimeMillis();
                    long tDelta5 = tEnd5 - tStart5;
                    consumerTimeM5 += tDelta5;
                    applier.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, this);
                }
                else {
                    LOGGER.warn("Unexpected query event: " + querySQL);
                }
                break;

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                if (((TableMapEvent) event).getTableName().toString().equals("db_heartbeat")) {
                    // reset the fake microsecond counter on hearth beat event. In our case
                    // hearth-beat is a regular update and it is treated as such in the rest
                    // of the code (therefore replicated in HBase table so we have the
                    // hearth-beat in HBase and can use it to check replication delay). The only
                    // exception is that when we see this event we reset the fake-microseconds counter.
                    fakeMicrosecondCounter = 0;
                    injectFakeMicroSecondsIntoEventTimestamp(event);
                } else {
                    fakeMicrosecondCounter++;
                    injectFakeMicroSecondsIntoEventTimestamp(event);
                }
                try {
                    currentTransactionMetadata.updateCache((TableMapEvent) event);
                    long tableID = ((TableMapEvent) event).getTableId();
                    String tableName = ((TableMapEvent) event).getTableName().toString();
                    String dbName = currentTransactionMetadata.getDBNameFromTableID(tableID);
                    String hbaseTableName = dbName.toLowerCase()
                            + ":"
                            + tableName.toLowerCase();
                    if (configuration.getApplierType().equals("hbase")) {
                        if (!hBaseSchemaManager.isTableKnownToHBase(hbaseTableName)) {
                            // This should not happen in tableMapEvent, unless we are
                            // replaying the binlog.
                            // TODO: load hbase tables on start-up so this never happens
                            hBaseSchemaManager.createHBaseTableIfNotExists(hbaseTableName);
                        }
                    }
                    updateLastKnownPositionForMapEvent();
                }
                catch (Exception e) {
                    LOGGER.error("Could not execute mapEvent block. Something went wrong.");
                    e.printStackTrace();
                    System.exit(1);
                }
                break;

            // Data event:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                incUpdateCounter();
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.apply(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                break;

            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
                incInsertCounter();
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.apply(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                break;

            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                incDeleteCounter();
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.apply(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                break;

            case MySQLConstants.XID_EVENT:
                // Latter we may want to tag previous data events with xid_id
                // (so we can know if events were in the same transaction).
                // For now we just increase the counter.
                fakeMicrosecondCounter++;
                incXIDCounter();
                injectFakeMicroSecondsIntoEventTimestamp(event);
                applier.applyXIDEvent((XidEvent) event, this);
                break;

            // Events that we expect to appear in the binlog, but we don't do
            // any extra processing.
            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
            case MySQLConstants.ROTATE_EVENT:
            case MySQLConstants.STOP_EVENT:
                break;

            // Events that we do not expect to appear in the binlog
            // so a warning should be logged for those types
            default:
                LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
                break;
        }

    }

    public boolean isDDL(String querySQL) {

        long tStart = System.currentTimeMillis();
        boolean isDDL;
        // optimization
        if (querySQL.equals("BEGIN")) {
            isDDL = false;
        }
        else {

            String ddlPattern = "(alter|drop|create|rename|truncate|modify)\\s+(table|column)";

            Pattern p = Pattern.compile(ddlPattern, Pattern.CASE_INSENSITIVE);

            Matcher m = p.matcher(querySQL);

            isDDL = m.find();
        }

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;

        consumerTimeM2 += tDelta;
        return isDDL;
    }

    public boolean isCOMMIT(String querySQL) {

        boolean isCOMMIT;
        // optimization
        if (querySQL.equals("BEGIN")) {
            isCOMMIT = false;
        }
        else {

            String commitPattern = "(commit)";

            Pattern p = Pattern.compile(commitPattern, Pattern.CASE_INSENSITIVE);

            Matcher m = p.matcher(querySQL);

            isCOMMIT = m.find();
        }
        return isCOMMIT;
    }

    public boolean isReplicant(String schemaName) {
        if (schemaName.equals(configuration.getReplicantSchemaName())) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean isCreateOnly(String querySQL) {

        // TODO: use this to skip table create for tables that allready exists

        boolean isCreateOnly;

        String createPattern = "(create)\\s+(table)";
        Pattern pC = Pattern.compile(createPattern, Pattern.CASE_INSENSITIVE);
        Matcher mC = pC.matcher(querySQL);

        boolean hasCreate = mC.find();

        String otherPattern = "(alter|drop|rename|truncate|modify)\\s+(table|column)";
        Pattern pO = Pattern.compile(otherPattern, Pattern.CASE_INSENSITIVE);
        Matcher mO = pO.matcher(querySQL);

        boolean hasOther = mO.find();

        if (hasCreate && !hasOther) {
            isCreateOnly = true;
        }
        else {
            isCreateOnly = false;
        }
        return isCreateOnly;
    }

    /**
     * Returns true if event type is not tracked, or does not belong to the
     * tracked database
     * @param  event Binlog event that needs to be checked
     * @return shouldSkip Weather event should be skipped or processed
     * @throws TableMapException
     */
    public boolean skipEvent(BinlogEventV4 event) throws TableMapException {

        long tStart = System.currentTimeMillis();

        boolean eventIsTracked      = false;

        switch (event.getHeader().getEventType()) {
            // Query Event:
            case MySQLConstants.QUERY_EVENT:
                String querySQL  = ((QueryEvent) event).getSql().toString();
                boolean isDDL    = isDDL(querySQL);
                boolean isCOMMIT = isCOMMIT(querySQL);
                String dbName = ((QueryEvent) event).getDatabaseName().toString();

                if (isReplicant(dbName) && (isDDL || isCOMMIT)) {
                    eventIsTracked = true;
                }
                else {
                    if (!querySQL.equals("BEGIN")) {
                        LOGGER.warn("Received non-DDL, non-COMMIT, non-BEGIN statement: " + querySQL);
                    }
                }
                break;

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                if (isReplicant(((TableMapEvent)event).getDatabaseName().toString())) {
                    eventIsTracked = true;
                }
                else{
                    eventIsTracked = false;
                }
                break;

            // Data event:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                if (currentTransactionMetadata.getFirstMapEventInTransaction() == null) {
                    // row event and no map event -> blacklisted schema so map event was skipped
                    eventIsTracked = false;
                }
                else {
                    eventIsTracked = true;
                }
                break;

            case MySQLConstants.XID_EVENT:
                if (currentTransactionMetadata.getFirstMapEventInTransaction() == null) {
                    // xid event and no map event -> blacklisted schema so map event was skipped
                    eventIsTracked = false;
                }
                else {
                    eventIsTracked = true;
                }
                break;

            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
            case MySQLConstants.ROTATE_EVENT:
            case MySQLConstants.STOP_EVENT:
                eventIsTracked = true;
                break;

            default:
                eventIsTracked = false;
                break;
        }

        boolean skipEvent;
        skipEvent = !eventIsTracked;

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        consumerTimeM3 += tDelta;

        return  skipEvent;
    }

    public static long getFakeMicrosecondCounter() {
        return fakeMicrosecondCounter;
    }

    private void injectFakeMicroSecondsIntoEventTimestamp(BinlogEventV4 event) {

        long tStart = System.currentTimeMillis();

        long overriddenTimestamp = event.getHeader().getTimestamp();

        if (overriddenTimestamp != 0) {
            // timestamp is in millisecond form, but the millisecond part is actually 000 (for example 1447755881000)
            String timestampString = Long.toString(overriddenTimestamp).substring(0,10);
            overriddenTimestamp = Long.parseLong(timestampString) * 1000000;
            overriddenTimestamp += fakeMicrosecondCounter;
            ((BinlogEventV4HeaderImpl)(event.getHeader())).setTimestamp(overriddenTimestamp);
        }
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;

        consumerTimeM4 += tDelta;
    }

    private void updateLastKnownPositionForMapEvent() {

        TableMapEvent event = currentTransactionMetadata.getFirstMapEventInTransaction();

        lastKnownInfo.put(
                Constants.LAST_KNOWN_MAP_EVENT_POSITION,
                new BinlogPositionInfo(
                        event.getBinlogFilename(),
                        event.getHeader().getPosition()
                )
        );

        LOGGER.info("Updated last known map event position to => ["
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename()
                + ":"
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogPosition()
                + "]"
        );
    }

    // TODO: make this nicer
    private void updateLastKnownFakeMicrosecondCounterForMapEvent() {

        TableMapEvent event = currentTransactionMetadata.getFirstMapEventInTransaction();

        lastKnownInfo.put(
                Constants.LAST_KNOWN_MAP_EVENT_POSITION_FAKE_MICROSECONDS_COUNTER,
                new BinlogPositionInfo(
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        fakeMicrosecondCounter
                )
        );

        LOGGER.info("Updated last known map event fakeMicrosendsCounter info to => ["
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION_FAKE_MICROSECONDS_COUNTER)).getBinlogFilename()
                + ":"
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION_FAKE_MICROSECONDS_COUNTER)).getBinlogPosition()
                + ":"
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION_FAKE_MICROSECONDS_COUNTER)).getFakeMicrosecondsCounter()
                + "]"
        );
    }

    private void updateLastKnownPosition(AbstractRowEvent event) {
        BinlogPositionInfo lastKnownPosition = new BinlogPositionInfo(
                event.getBinlogFilename(),
                event.getHeader().getPosition()
        );
        lastKnownInfo.put(Constants.LAST_KNOWN_BINLOG_POSITION, lastKnownPosition);
    }

    public ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> getPipelineStats() {
        return pipelineStats;
    }

    private void checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);

        if (this.pipelineStats.containsKey(currentTimeSeconds)) {
            return;
        }
        else {
            pipelineStats.put(currentTimeSeconds, new HashMap<Integer, MutableLong>());
            pipelineStats.get(currentTimeSeconds).put(Counters.INSERT_COUNTER, new MutableLong());
            pipelineStats.get(currentTimeSeconds).put(Counters.UPDATE_COUNTER, new MutableLong());
            pipelineStats.get(currentTimeSeconds).put(Counters.DELETE_COUNTER, new MutableLong());
            pipelineStats.get(currentTimeSeconds).put(Counters.COMMIT_COUNTER, new MutableLong());
            pipelineStats.get(currentTimeSeconds).put(Counters.XID_COUNTER, new MutableLong());
        }
    }

    private void incInsertCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (pipelineStats.get(currentTimeSeconds) == null) {
            checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists();
        }
        pipelineStats.get(currentTimeSeconds).get(Counters.INSERT_COUNTER).increment();
    }

    private void incUpdateCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (pipelineStats.get(currentTimeSeconds) == null) {
            checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists();
        }
        pipelineStats.get(currentTimeSeconds).get(Counters.UPDATE_COUNTER).increment();
    }

    private void incDeleteCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (pipelineStats.get(currentTimeSeconds) == null) {
            checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists();
        }
        pipelineStats.get(currentTimeSeconds).get(Counters.DELETE_COUNTER).increment();
    }

    private void incCommitQueryCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (pipelineStats.get(currentTimeSeconds) == null) {
            checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists();
        }
        pipelineStats.get(currentTimeSeconds).get(Counters.COMMIT_COUNTER).increment();
    }

    private void incXIDCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (pipelineStats.get(currentTimeSeconds) == null) {
            checkPipelineStatsForCurrentSecondKeyAndAddIfKeyDoesNotExists();
        }
        pipelineStats.get(currentTimeSeconds).get(Counters.XID_COUNTER);
    }
}
