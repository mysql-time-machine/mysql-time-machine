package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.util.MutableLong;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class STDOUTJSONApplier implements Applier {

    private static  long totalEventsCounter = 0;
    private static  long totalRowsCounter = 0;

    private static final HashMap<String,MutableLong> stats = new HashMap<String,MutableLong>();

    private static final Logger LOGGER = LoggerFactory.getLogger(STDOUTJSONApplier.class);

    @Override
    public void applyXIDEvent(XidEvent event) {
        for (String table : stats.keySet()) {
            LOGGER.info("XID, current stats: { table => " + table + ", rows => " + stats.get(table).getValue());
        }
    }

    @Override
    public void bufferData(AugmentedRowsEvent augmentedRowsEvent, PipelineOrchestrator caller) {
        totalEventsCounter++;
        LOGGER.info("STDOUT Applier received event: number of rows in event => " +  augmentedRowsEvent.getSingleRowEvents().size());
        // TODO: make this optional with --verbose
        for (AugmentedRow row : augmentedRowsEvent.getSingleRowEvents()) {
            String tableName = row.getTableName();
            if (tableName != null) {
                if (stats.containsKey(tableName)) {
                    stats.get(tableName).increment();
                }
                else {
                    stats.put(tableName, new MutableLong());
                }
            }
            else {
                LOGGER.error("table name can not be null");
            }
            totalRowsCounter++;
            if((totalRowsCounter % 10000) == 0) {
                LOGGER.info("totalRowsCounter => " + totalRowsCounter);
                for (String table : stats.keySet()) {
                    LOGGER.info("{ table => " + table + ", rows => " + stats.get(table).getValue());
                }
            }
            System.out.println(row.toJSON());
        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {
        LOGGER.info("COMMIT");
        for (String table : stats.keySet()) {
            LOGGER.info("COMMIT, current stats: { table => " + table + ", rows => " + stats.get(table).getValue());
        }
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {
        totalEventsCounter++;
        String json = augmentedSchemaChangeEvent.toJSON();
        // TODO: make this options with --verbose
        //if (json != null) {
        //   System.out.println("STDOUT Applier serving augmentedSchemaChangeEvent: \n" + json);
        //}
        //else {
        //    System.out.println("Received empty event");
        //}
    }
}
