package com.booking.validator.service.task;

import com.booking.validator.service.Service;
import com.booking.validator.service.protocol.ValidationTaskDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

/**
 * A thread safe validation task suppler fetching tasks from a kafka queue
 *
 * TODO: fetch tasks ahead of need
 *
 * Created by psalimov on 9/9/16.
 */
public class KafkaTaskDescriptionSupplier implements Supplier<ValidationTaskDescription>, Service {

    private final KafkaConsumer<String, ValidationTaskDescription> consumer;

    private Queue<ValidationTaskDescription> tasks = new ConcurrentLinkedQueue<>();

    private Object lock = new Object();

    public static KafkaTaskDescriptionSupplier getInstance(Map<String,Object> configs){

        KafkaConsumer<String, ValidationTaskDescription> consumer = new KafkaConsumer<>( configs );

        return new KafkaTaskDescriptionSupplier( consumer );

    }

    public KafkaTaskDescriptionSupplier(KafkaConsumer<String, ValidationTaskDescription> consumer) {
        this.consumer = consumer;
    }

    @Override
    public ValidationTaskDescription get() {

        ValidationTaskDescription task = tasks.poll();

        // Kafka consumer is not thread safe, but it returns a bunch of records that can be processed in parallel
        while (task == null){

            synchronized (lock){

                // an another thread could already have done the hard job of polling for us
                task = tasks.poll();

                if ( task == null ){
                    ConsumerRecords<String, ValidationTaskDescription> records = consumer.poll(Long.MAX_VALUE);

                    for (ConsumerRecord<String, ValidationTaskDescription> record : records){
                        tasks.add(record.value());
                    }

                    task = tasks.poll();
                }

            }

        }

        return task;
    }

    @Override
    public void start() {

    }
}
