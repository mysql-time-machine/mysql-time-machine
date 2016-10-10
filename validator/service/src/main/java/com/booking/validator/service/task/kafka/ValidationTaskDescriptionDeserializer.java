package com.booking.validator.service.task.kafka;

import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by psalimov on 10/3/16.
 */
public class ValidationTaskDescriptionDeserializer implements Deserializer<ValidationTaskDescription> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationTaskDescriptionDeserializer.class);

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public ValidationTaskDescription deserialize(String s, byte[] bytes) {
        try {
            return  mapper.readValue(bytes, ValidationTaskDescription.class);
        } catch (IOException e) {

            LOGGER.error("Error deserializing task description", e);

            return null;
        }
    }

    @Override
    public void close() {}
}
