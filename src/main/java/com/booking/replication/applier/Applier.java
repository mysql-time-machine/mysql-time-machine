package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import java.io.IOException;

/**
 * Created by bosko on 11/14/15.
 */
public interface Applier {

    void apply(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) throws IOException;

    void applyCommitQueryEvent(QueryEvent event, PipelineOrchestrator caller);

    void applyXIDEvent(XidEvent event, PipelineOrchestrator caller);

    void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller);

}
