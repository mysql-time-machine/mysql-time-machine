package com.booking.validation;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Created by lezhong on 8/18/16.
 */

public class TestRunner {
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(TestRunner.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }

        System.out.println(result.wasSuccessful());
    }
}
