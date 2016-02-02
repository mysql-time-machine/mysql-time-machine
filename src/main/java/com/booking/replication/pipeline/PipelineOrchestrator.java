package com.booking.replication.pipeline;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
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
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.schema.HBaseSchemaManager;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.applier.HBaseApplier;
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

    // private final ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> pipelineStats;

    private final ReplicatorMetrics replicatorMetrics;

    public CurrentTransactionMetadata currentTransactionMetadata;

    private volatile boolean running = false;

    private volatile boolean replicatorShutdownRequested = false;

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

    public void requestReplicatorShutdown(){
        this.replicatorShutdownRequested = true;
    }

    public boolean isReplicatorShutdownRequested() {
        return replicatorShutdownRequested;
    }

    public static void setFakeMicrosecondCounter(Long fakeMicrosecondCounter) {
        PipelineOrchestrator.fakeMicrosecondCounter = fakeMicrosecondCounter;
    }

    public PipelineOrchestrator(
            LinkedBlockingQueue<BinlogEventV4> q,
            ConcurrentHashMap<Integer,Object> chm,
            Configuration repcfg,
            ReplicatorMetrics replicatorMetrics
        ) throws SQLException, URISyntaxException, IOException {

        // this.pipelineStats = pStats;

        this.replicatorMetrics = replicatorMetrics;

        this.configuration = repcfg;

        eventAugmenter = new EventAugmenter(repcfg, replicatorMetrics);

        currentTransactionMetadata = new CurrentTransactionMetadata();

        if (configuration.getApplierType().equals("hbase")) {
            hBaseSchemaManager = new HBaseSchemaManager(repcfg.getZOOKEEPER_QUORUM());
        }

        queue = q;

        if (repcfg.getApplierType().equals("STDOUT")) {
            applier = new STDOUTJSONApplier();
        }
        else if (repcfg.getApplierType().toLowerCase().equals("hbase")) {
            applier = new HBaseApplier(repcfg.getZOOKEEPER_QUORUM(), replicatorMetrics);
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
                        replicatorMetrics.incEventsReceivedCounter();
                        if (!skipEvent(event)) {
                            calculateAndPropagateChanges(event);
                            replicatorMetrics.incEventsProcessedCounter();
                        }
                        else {
                            replicatorMetrics.incEventsSkippedCounter();
                        }
                        if (eventCounter % 5000 == 0) {
                            LOGGER.info("Pipeline report: producer queue size => " + queue.size());
                        }
                    } else {
                        LOGGER.warn("Poll timeout. Will sleep for 1s and try again.");
                        Thread.sleep(1000);
                    }
                }
                else {
                    LOGGER.info("Pipeline report: no items in producer event queue. Will sleep for 0.5s and check again.");
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException, requesting replicator shutdown...", e);
                requestReplicatorShutdown();
            } catch (TableMapException e) {
                LOGGER.error("TableMapException, requesting replicator shutdown...",e);
                requestReplicatorShutdown();
            } catch (IOException e) {
                LOGGER.error("IOException, requesting replicator shutdown...",e);
                requestReplicatorShutdown();
            } catch (Exception e) {
                LOGGER.error("Exception, requesting replicator shutdown...",e);
                requestReplicatorShutdown();
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

        long tStart;
        long tEnd;
        long tDelta;

        switch (event.getHeader().getEventType()) {

            // DDL Event:
            case MySQLConstants.QUERY_EVENT:
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                String querySQL = ((QueryEvent) event).getSql().toString();
                if (isCOMMIT(querySQL)) {
                    replicatorMetrics.incCommitQueryCounter();
                    applier.applyCommitQueryEvent((QueryEvent) event);
                    currentTransactionMetadata = null;
                    currentTransactionMetadata = new CurrentTransactionMetadata();
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
                String tableName = ((TableMapEvent) event).getTableName().toString();
                if (tableName.equals(Constants.HEART_BEAT_TABLE)) {
                    // reset the fake microsecond counter on hearth beat event. In our case
                    // hearth-beat is a regular update and it is treated as such in the rest
                    // of the code (therefore replicated in HBase table so we have the
                    // hearth-beat in HBase and can use it to check replication delay). The only
                    // exception is that when we see this event we reset the fake-microseconds counter.
                    LOGGER.debug("fakeMicrosecondCounter before reset => " + fakeMicrosecondCounter);
                    fakeMicrosecondCounter = 0;
                    injectFakeMicroSecondsIntoEventTimestamp(event);
                    replicatorMetrics.incHeartBeatCounter();
                } else {
                    fakeMicrosecondCounter++;
                    injectFakeMicroSecondsIntoEventTimestamp(event);
                }
                try {
                    currentTransactionMetadata.updateCache((TableMapEvent) event);
                    long tableID = ((TableMapEvent) event).getTableId();
                    String dbName = currentTransactionMetadata.getDBNameFromTableID(tableID);
                    LOGGER.debug("processing events for { db => " + dbName + " table => " + tableName + " } ");
                    LOGGER.debug("fakeMicrosecondCounter at tableMap event => " + fakeMicrosecondCounter);
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
                    updateLastKnownPositionForMapEvent((TableMapEvent) event);
                }
                catch (Exception e) {
                    LOGGER.error("Could not execute mapEvent block. Requesting replicator shutdown...", e);
                    requestReplicatorShutdown();
                }
                break;

            // Data event:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.bufferData(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                replicatorMetrics.incUpdateEventCounter();
                break;

            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.bufferData(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                replicatorMetrics.incInsertEventCounter();
                break;

            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.bufferData(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                replicatorMetrics.incDeleteEventCounter();
                break;

            case MySQLConstants.XID_EVENT:
                // Latter we may want to tag previous data events with xid_id
                // (so we can know if events were in the same transaction).
                // For now we just increase the counter.
                fakeMicrosecondCounter++;
                injectFakeMicroSecondsIntoEventTimestamp(event);
                applier.applyXIDEvent((XidEvent) event);
                replicatorMetrics.incXIDCounter();
                currentTransactionMetadata = null;
                currentTransactionMetadata = new CurrentTransactionMetadata();
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
                if (isReplicant(dbName)) {
                    if ((isDDL || isCOMMIT)) {
                        eventIsTracked = true;
                    }
                    else {
                        if (!querySQL.equals("BEGIN")) {
                            eventIsTracked = false;
                            LOGGER.warn("Received non-DDL, non-COMMIT, non-BEGIN statement: " + querySQL);
                        }
                    }
                }
                else {
                    // LOGGER.debug("non-replicant db event for db: " + dbName);
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

    private void updateLastKnownPositionForMapEvent(TableMapEvent event) {

        String lastBinlogFileName;
        if (lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION) != null) {
            lastBinlogFileName = ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename();
        }
        else {
            lastBinlogFileName = "";
        }

        if (!event.getBinlogFilename().equals(lastBinlogFileName)) {
            LOGGER.info("moving to next binlog file [ " + lastBinlogFileName + " ] ==>> [ " + event.getBinlogFilename() + " ]");
        }

        lastKnownInfo.put(
                Constants.LAST_KNOWN_MAP_EVENT_POSITION,
                new BinlogPositionInfo(
                        event.getBinlogFilename(),
                        event.getHeader().getPosition()
                )
        );

        LOGGER.debug("Updated last known map event position to => ["
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename()
                + ":"
                + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogPosition()
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
}
