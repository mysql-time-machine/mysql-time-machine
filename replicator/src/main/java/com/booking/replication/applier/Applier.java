package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

/**
 * Created by bosko on 11/14/15.
 */
public abstract class Applier {

    public abstract void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller);

    public void applyCommitQueryEvent(QueryEvent event) {}

    public void applyXidEvent(XidEvent event) {}

    public void applyRotateEvent(RotateEvent event) {}

    public void applyTableMapEvent(TableMapEvent event) {}

    public void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller) {}

    public abstract void forceFlush();

    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {}

    public abstract void waitUntilAllRowsAreCommitted(BinlogEventV4 event);
}
