package com.booking.validation;

import static java.lang.StrictMath.abs;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by lezhong on 7/14/16.
 */


public class Validating {
    final double smallValue = 0.0001;
    /*
        MySQL type list:
            char,
            varchar,
            tinyext,
            text,
            blob,
            mediumtext,
            mediumblob,
            longtext,
            longblob,
            enum(x, y, z, etc.)
            set

        HBase type list:
            row key, column family, column qualifier, timestamp, value
    */

    Boolean compareGeneral(String valueMySQL, String valueNonMySQL) {
        // Double, TinyInt, SmallInt, MediumInt, Int, BigInt, Decimal, Enum, Set,
        return (valueMySQL == null && valueNonMySQL.equals(("NULL"))) || valueMySQL.equals(valueNonMySQL);
    }

    Boolean compareFloat(String valueMySQL, String valueNonMySQL) {
        if (valueMySQL == null) {
            return valueNonMySQL.equals("NULL");
        } else {
            if (valueNonMySQL.equals("NULL")) {
                return false;
            }
        }
        float floatMySQL = Float.parseFloat(valueMySQL);
        float floatHBase = Float.parseFloat(valueNonMySQL);
        return abs(floatHBase - floatMySQL) < smallValue;
    }

    Boolean compareText(String valueMySQL, String valueNonMySQL) {
        if (valueMySQL == null) {
            return valueNonMySQL.equals("NULL");
        }
        return valueMySQL.equals(valueNonMySQL);
    }

    Boolean compareTimestamp(String valueMySQL, String valueNonMySQL) {
        if (valueMySQL == null) {
            return valueNonMySQL.equals("NULL");
        } else {
            if (valueNonMySQL.equals("NULL")) {
                return false;
            }
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.valueOf(valueNonMySQL));
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        format.setTimeZone(TimeZone.getTimeZone("Europe/Amsterdam"));
        String converted = format.format(calendar.getTime());
        return valueMySQL.substring(0, valueMySQL.length() - 3).equals(converted.substring(0, converted.length() - 3));
        // In case there is one second difference, we consider they are equal as well.
    }

    Boolean compareDate(String valueMySQL, String valueNonMySQL) {
        if (valueMySQL == null) {
            return valueNonMySQL.equals("NULL");
        } else {
            if (valueNonMySQL.equals("NULL")) {
                return false;
            }
        }
        return valueMySQL.equals(valueNonMySQL);
    }

    Boolean typeHelper(String type, String typename) {
        return type.length() >= typename.length() && type.substring(0, typename.length()).equals(typename);
    }

    Boolean comparisonHelper(String type, String valueMySQL, String valueNonMySQL) {
        if (typeHelper(type, "tinyint") || typeHelper(type, "int") || typeHelper(type, "char")
                || typeHelper(type, "varchar") || typeHelper(type, "bigint")) {
            return compareGeneral(valueMySQL, valueNonMySQL);
        }
        if (typeHelper(type, "float") || typeHelper(type, "double")) {
            return compareFloat(valueMySQL, valueNonMySQL);
        }
        if (typeHelper(type, "text")) {
            return compareText(valueMySQL, valueNonMySQL);
        }
        if (typeHelper(type, "timestamp")) {
            return compareTimestamp(valueMySQL, valueNonMySQL);
        }
        if (typeHelper(type, "date")) {
            return compareDate(valueMySQL, valueNonMySQL);
        }
        if (typeHelper(type, "enum")) {
            return compareGeneral(valueMySQL, valueNonMySQL);
        } else {
            System.out.println("Please add " + type + "!!!");
        }
        return true;
    }
}
