package com.booking.validation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Stack;

/**
 * Created by lezhong on 8/12/16.
 */

public class KafkaConnector {
    private ConfigurationKafka config = new ConfigurationKafka();
    private String topicName;
    private KafkaConsumer<String, String> consumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnector.class);
    private Stack<JSONObject> messageStack =  new Stack<>();

    private static Properties getConsumerProperties(String broker) {
        // Consumer configuration
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("group.id", "getLastCommittedMessages");
        // prop.put("auto.offset.reset", "latest");
        prop.put("enable.auto.commit", "false");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return prop;
    }

    private String getNewMessage() throws IOException {
        // Method to fetch the last committed message in each partition of each topic.
        final int RetriesLimit = 100;
        final int POLL_TIME_OUT = 1000;
        ConsumerRecord<String, String> lastMessage;
        ConsumerRecords<String, String> messages;

        // loop partitions
        for (PartitionInfo pi: consumer.partitionsFor(topicName)) {
            TopicPartition partition = new TopicPartition(topicName, pi.partition());
            consumer.assign(Collections.singletonList(partition));
            LOGGER.info("Position: " + String.valueOf(consumer.position(partition)));
            long endPosition = consumer.position(partition);

            // There is an edge case here. With a brand new partition, consumer position is equal to 0
            if (endPosition > 0) {
                int retries = 0;
                while (retries < RetriesLimit) {
                    messages = consumer.poll(POLL_TIME_OUT);
                    if (!messages.isEmpty()) {
                        lastMessage = messages.iterator().next();
                        return lastMessage.value();
                    }
                    retries++;
                }
            }
        }
        return null;
    }

    public JSONObject nextKeyValue() {
        try {
            if (messageStack.size() == 0) {
                String messages = getNewMessage();
                JSONParser parser = new JSONParser();
                try {
                    JSONObject valObj = (JSONObject) parser.parse(messages);
                    JSONArray mesObj = (JSONArray) valObj.get("rows");
                    for (Object message: mesObj) {
                        messageStack.push((JSONObject) message);
                    }
                } catch (ParseException pa) {
                    pa.printStackTrace();
                }
            }
            return messageStack.pop();
        } catch (IOException io) {
            io.printStackTrace();
        }
        return new JSONObject();
    }

    KafkaConnector() {
        String brokerAddress;

        brokerAddress = config.getKafkaBroker();
        System.out.println("------------------" + brokerAddress);
        topicName = config.getKafkaTopicName();
        consumer = new KafkaConsumer<>(getConsumerProperties(brokerAddress));
    }
}

