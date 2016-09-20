package com.booking.validator.service;

import com.booking.validator.data.DataPointerFactory;
import com.booking.validator.data.HBaseDataPointerFactory;
import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.booking.validator.service.task.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by psalimov on 9/2/16.
 */
public class Launcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Launcher.class);

    private static final String HBASE = "hbase";
    private static final String MYSQL = "mysql";
    private static final String KAFKA = "kafka";

    public static void main(String[] args) {

        ValidatorConfiguration validatorConfiguration = new ValidatorConfiguration();

        System.out.println("Validator service starting");

        Launcher launcher = new Launcher(validatorConfiguration);

        launcher.launch();


        System.out.println("Validator service started");

    }

    private final ValidatorConfiguration validatorConfiguration;

    public Launcher(ValidatorConfiguration validatorConfiguration){

        this.validatorConfiguration = validatorConfiguration;
    }

    public void launch(){

        Validator validator = new Validator(getTaskSupplier(), getResultConsumer(), getErrorConsumer());

        validator.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }

    private Consumer<ValidationTaskResult> getResultConsumer(){
        return x -> System.out.println(x);
    }

    private Consumer<Throwable> getErrorConsumer(){
        return x -> System.out.println(x);
    }

    private Supplier<ValidationTask> getTaskSupplier(){

        ValidatorConfiguration.TaskSupplier supplierDescription = validatorConfiguration.getTaskSupplier();

        Supplier<ValidationTaskDescription> supplier;

        if (KAFKA.equals( supplierDescription.getType() )){

            supplier = getKafkaTaskDescriptionSupplier( supplierDescription.getConfiguration() );

        } else {

            supplier = new CommandLineTaskDescriptionSupplier();

        }

        return new TaskSupplier( supplier, getDataPointers() );
    }

    private KafkaTaskDescriptionSupplier getKafkaTaskDescriptionSupplier( Map<String,String> configuration ){

        return KafkaTaskDescriptionSupplier.getInstance(null);
    }

    private DataPointers getDataPointers(){

        Map<String, DataPointerFactory<?,?>> knownFactories = new HashMap<>();

        Map<String, List<ValidatorConfiguration.DataSource>> sources = StreamSupport.stream( validatorConfiguration.getDataSources().spliterator(), false )
                .collect( Collectors.groupingBy( source -> source.getName() ) );

        DataPointerFactory hbaseFactory = getHBaseFactory( sources.getOrDefault( HBASE, Collections.EMPTY_LIST ) );

        knownFactories.put(HBASE, hbaseFactory);

        return new DataPointers(knownFactories);
    }

    private DataPointerFactory<?,?> getHBaseFactory( Iterable<ValidatorConfiguration.DataSource> sources ){

        Map<String,Connection> hbaseConnections = new HashMap<>();

        for (ValidatorConfiguration.DataSource source : sources ){

            Configuration configuration = HBaseConfiguration.create();

            source.getConfiguration().forEach( (key,value) -> configuration.set(key,value) );

            try ( Connection connection = ConnectionFactory.createConnection(configuration)) {

                hbaseConnections.put( source.getName(), connection );

            } catch (IOException e) {

                LOGGER.error("Failed to connect to HBase cluster "+ source.getName(), e );

            }

        }

        return new HBaseDataPointerFactory( hbaseConnections );

    }

}
