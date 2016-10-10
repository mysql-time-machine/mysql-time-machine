package com.booking.validator.service;

import com.booking.validator.data.constant.ConstDataPointerFactory;
import com.booking.validator.data.DataPointerFactory;
import com.booking.validator.data.hbase.HBaseDataPointerFactory;
import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.booking.validator.service.task.*;
import com.booking.validator.service.task.cli.CommandLineValidationTaskDescriptionSupplier;
import com.booking.validator.service.task.kafka.KafkaValidationTaskDescriptionSupplier;
import com.booking.validator.service.utils.CommandLineArguments;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

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
    private static final String CONST = "const";
    private static final String MYSQL = "mysql";
    private static final String KAFKA = "kafka";

    public static void main(String[] args) {

        LOGGER.info("Starting validator service...");

        CommandLineArguments command = new CommandLineArguments(args);

        ValidatorConfiguration validatorConfiguration;

        try {

            validatorConfiguration = ValidatorConfiguration.fromFile( command.getConfigurationPath() );

        } catch (IOException e) {

            LOGGER.error("Failed reading configuration file", e);

            return;

        }

        new Launcher( validatorConfiguration ).launch();

        LOGGER.info("Validator service started.");

        try {
            for(;;) Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            return;
        }

    }

    private final ValidatorConfiguration validatorConfiguration;

    public Launcher(ValidatorConfiguration validatorConfiguration){

        this.validatorConfiguration = validatorConfiguration;
    }

    public void launch(){

        new Validator( getTaskSupplier(), getResultConsumer(), getErrorConsumer() ).start();

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

            supplier = new CommandLineValidationTaskDescriptionSupplier();

        }

        return new TaskSupplier( supplier, getDataPointers() );
    }

    private KafkaValidationTaskDescriptionSupplier getKafkaTaskDescriptionSupplier(Map<String,String> configuration ){

        String topic = configuration.remove("topic");

        Properties properties = new Properties();

        configuration.entrySet().stream().forEach( x -> properties.setProperty(x.getKey(), x.getValue()) );

        return KafkaValidationTaskDescriptionSupplier.getInstance( topic , properties );
    }

    private DataPointers getDataPointers(){

        Map<String, DataPointerFactory> knownFactories = new HashMap<>();

        Map<String, List<ValidatorConfiguration.DataSource>> sources = StreamSupport.stream( validatorConfiguration.getDataSources().spliterator(), false )
                .collect( Collectors.groupingBy( source -> source.getName() ) );

        DataPointerFactory hbaseFactory = getHBaseFactory( sources.getOrDefault( HBASE, Collections.EMPTY_LIST ) );

        knownFactories.put(HBASE, hbaseFactory);
        knownFactories.put(CONST, new ConstDataPointerFactory());

        return new DataPointers(knownFactories);
    }

    private DataPointerFactory getHBaseFactory( Iterable<ValidatorConfiguration.DataSource> sources ){

        Map<String,Configuration> hbaseConfigurations = StreamSupport.stream( sources.spliterator(), false )
                .collect( Collectors.toMap(
                        s -> s.getName(),
                        s-> {

                            Configuration configuration = HBaseConfiguration.create();

                            s.getConfiguration().forEach( (key,value) -> configuration.set(key,value) );

                            return configuration;

                        } ) );

        return HBaseDataPointerFactory.getInstance(hbaseConfigurations);

    }

}
