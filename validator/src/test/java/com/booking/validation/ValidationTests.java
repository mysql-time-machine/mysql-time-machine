package com.booking.validation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by lezhong on 8/18/16.
 */

public class ValidationTests {

    @Test
    public void multiplicationOfZeroIntegersShouldReturnZero() {
        ValidationRule validator = new ValidationRule(); // Validation is tested

        // assert statements
        assertEquals(true, validator.comparisonHelper("char", "abc", "abc"));
    }
}
