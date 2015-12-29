package com.booking.replication;

import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.booking.replication.monitor.Overseer;

import com.booking.replication.util.MutableLong;
import com.google.code.or.binlog.BinlogEventV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Booking replicator. Has two main objects (producer and consumer)
 * that reference the same thread-safe queue.
 * Producer pushes binlog events to the queue and consumer
 * reads them. Producer is basically a wrapper for open replicator,
 * and consumer is wrapper for all booking specific logic (schema
 * version control, augmenting events and storing events).
 */
public class Replicator {

    private final Configuration       configuration;
    private final BinlogEventProducer binlogEventProducer;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final Overseer overseer;

    private static final int MAX_QUEUE_SIZE = Constants.MAX_QUEUE_SIZE;

    private final LinkedBlockingQueue<BinlogEventV4> binlogEventQueue =
            new LinkedBlockingQueue<BinlogEventV4>(MAX_QUEUE_SIZE);

    private final ConcurrentHashMap<Integer,Object> lastKnownInfo =
            new ConcurrentHashMap<Integer, Object>();

    private final  ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> pipelineStats =
            new  ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>>();

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    // Replicator()
    public Replicator(Configuration conf) throws SQLException, URISyntaxException, IOException {
        configuration  = conf;

        BinlogPositionInfo lastKnownPosition = new BinlogPositionInfo(
            conf.getStartingBinlogFileName(),
            conf.getStartingBinlogPosition()
        );
        lastKnownInfo.put(Constants.LAST_KNOWN_BINLOG_POSITION, lastKnownPosition);

        binlogEventProducer = new BinlogEventProducer(binlogEventQueue, lastKnownInfo, configuration);
        pipelineOrchestrator = new PipelineOrchestrator( binlogEventQueue, lastKnownInfo, configuration, pipelineStats);

        overseer = new Overseer(binlogEventProducer, pipelineOrchestrator, lastKnownInfo);
    }

    // start()
    public void start() throws Exception {
        try {

            // Shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {

                    // Overseer
                    try {
                        overseer.stopMonitoring();
                        overseer.join();
                        LOGGER.info("Overseer thread succesfully stopped");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // Producer
                    try {
                        // let open replicator stop its own threads
                        binlogEventProducer.stop(1000, TimeUnit.MILLISECONDS);
                        if (!binlogEventProducer.getOr().isRunning()) {
                            LOGGER.info("Successfully stoped open replicator");
                        }
                        else {
                            LOGGER.warn("Failed to stop open replicator");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // Consumer
                    try {
                        pipelineOrchestrator.setRunning(false);
                        pipelineOrchestrator.join();
                        LOGGER.info("Consumer thread succesfully stopped");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            // Start up
            binlogEventProducer.start();
            pipelineOrchestrator.start();
            overseer.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // stop()
    public void stop(long timeout, TimeUnit unit) throws Exception {
        try {
            binlogEventProducer.stop(timeout,unit);
            pipelineOrchestrator.stopRunning();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public BinlogEventProducer getBinlogEventProducer() {
        return binlogEventProducer;
    }
}
