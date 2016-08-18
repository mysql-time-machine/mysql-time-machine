package com.booking.validation;

import com.booking.validation.Validating;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by lezhong on 8/18/16.
 */

public class ValidationTests {

    @Test
    public void multiplicationOfZeroIntegersShouldReturnZero() {
        Validating validator = new Validating(); // Validation is tested

        // assert statements
        assertEquals(true, validator.comparisonHelper("char", "abc", "abc"));
    }
}
