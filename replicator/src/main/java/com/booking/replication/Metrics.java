package com.booking.replication;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteRabbitMQ;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteSender;


import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;


public class Metrics {
    public static final MetricRegistry registry = new MetricRegistry();



    public static final Counter XIDCounter = Metrics.registry.counter("XIDCounter");
    public static final Counter deleteEventCounter = Metrics.registry.counter("deleteEventCounter");
    public static final Counter insertEventCounter = Metrics.registry.counter("insertEventCounter");
    public static final Counter incCommitQueryCounter = Metrics.registry.counter("incCommitQueryCounter");
    public static final Counter incUpdateEventCounter = Metrics.registry.counter("incUpdateEventCounter");

    public static final Counter incHeartBeatCounter = Metrics.registry.counter("incHeartBeatCounter");


    public static final Counter incEventsReceivedCounter = Metrics.registry.counter("incEventsReceivedCounter");
    public static final Counter incEventsProcessedCounter = Metrics.registry.counter("incEventsProcessedCounter");
    public static final Counter incEventsSkippedCounter = Metrics.registry.counter("incEventsSkippedCounter");

    public static final Counter incApplierTasksSubmittedCounter = Metrics.registry.counter("incApplierTasksSubmittedCounter");
    public static final Counter incApplierTasksSucceededCounter = Metrics.registry.counter("incApplierTasksSucceededCounter");
    public static final Counter incApplierTasksFailedCounter = Metrics.registry.counter("incApplierTasksFailedCounter");
    public static final Counter incApplierTasksInProgressCounter = Metrics.registry.counter("incApplierTasksInProgressCounter");

    public static final Counter incRowOpsCommittedToHbase = Metrics.registry.counter("incRowOpsCommittedToHbase");


//    public static

    public static PerTableMetricsHash perTableCounters = new PerTableMetricsHash("mysql");

    public static PerTableMetricsHash perHBaseTableCounters = new PerTableMetricsHash("hbase");

    public static class PerTableMetricsHash extends ConcurrentHashMap<String, PerTableMetrics>{

        private String prefix;

        public PerTableMetricsHash(String prefix) {
            super();
            this.prefix = prefix;
        }

        public PerTableMetrics getOrCreate(String key) {
            PerTableMetrics value;
            if(! this.containsKey(key)) {
                value = new Metrics.PerTableMetrics(prefix, key);
                this.put(key, value);
            } else {
                value = super.get(key);
            }
            return value;
        }
    }

    public static class PerTableMetrics {
        public Counter inserted;
        public Counter processed;
        public Counter deleted;
        public Counter updated;
        public Counter committed;

        public PerTableMetrics(String prefix, String tableName) {
            inserted    = Metrics.registry.counter(name(prefix, tableName, "inserted"));
            processed   = Metrics.registry.counter(name(prefix, tableName, "processed"));
            deleted     = Metrics.registry.counter(name(prefix, tableName, "deleted"));
            updated     = Metrics.registry.counter(name(prefix, tableName, "updated"));
            committed   = Metrics.registry.counter(name(prefix, tableName, "committed"));
        }
    }






//
//    Metrics() {
//        startReport();
//        Meter requests = registry.meter("requests");
//        requests.mark();
//    }

    private final static ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();


//    private final static GraphiteReporter greporter = GraphiteReporter
//            .forRegistry(registry)
////            .prefixedWith("user.rmirica.replicator")
////            .prefixedWith(Configuration.getGraphitPrefix())
//            .convertRatesTo(TimeUnit.SECONDS)
//            .convertDurationsTo(TimeUnit.SECONDS)
//            .filter(MetricFilter.ALL)
//            .build(new Graphite(new InetSocketAddress("localhost", 30002)));
//



    private static GraphiteReporter greporter;

    public static void setGraphiteReporter(Configuration conf) {
        if(greporter != null ) {
            throw new RuntimeException("You can only set up the Graphite reporter once.");
        }

        greporter = GraphiteReporter
            .forRegistry(registry)
            .prefixedWith(conf.getGraphiteStatsNamesapce())
//            .prefixedWith("user.rmirica.replicator")
//            .prefixedWith(Configuration.getGraphitPrefix())
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.SECONDS)
            .filter(MetricFilter.ALL)
            .build(new Graphite(new InetSocketAddress("localhost", 30002)));
    }






    public static void startReport() {
//        reporter = ConsoleReporter.forRegistry(registry)
//                .convertRatesTo(TimeUnit.SECONDS)
//                .convertDurationsTo(TimeUnit.MILLISECONDS)
//                .build();
        reporter.start(1, TimeUnit.SECONDS);
        greporter.start(10, TimeUnit.SECONDS);
    }

    public static void report() {
        reporter.report();
    }

}