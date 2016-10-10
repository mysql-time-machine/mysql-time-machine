package com.booking.validator.service.task.cli;

import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Scanner;
import java.util.function.Supplier;

/**
 * Created by psalimov on 9/21/16.
 */
public class CommandLineValidationTaskDescriptionSupplier implements Supplier<ValidationTaskDescription> {

    private ObjectMapper mapper = new ObjectMapper();
    private Scanner scanner = new Scanner(System.in);

    @Override
    public ValidationTaskDescription get() {

        try {
            return mapper.readValue( scanner.nextLine() , ValidationTaskDescription.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
