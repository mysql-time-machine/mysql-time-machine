package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class STDOUTJSONApplier implements Applier {

    private static  long eventCounter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(STDOUTJSONApplier.class);

    @Override
    public void applyXIDEvent(XidEvent event,PipelineOrchestrator caller) {

    }

    @Override
    public void apply(AugmentedRowsEvent augmentedRowsEvent, PipelineOrchestrator caller) {
        eventCounter++;
        LOGGER.info("Number of rows in event => " +  augmentedRowsEvent.getSingleRowEvents().size());
        for (AugmentedRow row : augmentedRowsEvent.getSingleRowEvents()) {
            System.out.println(row.toJSON());
        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event, PipelineOrchestrator caller) {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {
        eventCounter++;
        String json = augmentedSchemaChangeEvent.toJSON();
        if (json != null) {
           System.out.println("STDOUT Applier serving augmentedSchemaChangeEvent: \n" + json);
        }
        else {
            System.out.println("Received empty event");
        }
    }
}
