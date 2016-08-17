package com.booking.validation;

import static java.lang.StrictMath.abs;

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
        System.out.println(String.format("Compare %s with %s", valueMySQL, valueNonMySQL));
        return (valueMySQL == null && valueNonMySQL.equals(("NULL"))) || valueMySQL.equals(valueNonMySQL);
    }

    Boolean compareFloat(String valueMySQL, String valueNonMySQL) {
        System.out.println(valueMySQL + "==>" + valueNonMySQL);
        // int nrDigits = valueMySQL.split(".")[1].length();
        float floatMySQL = Float.parseFloat(valueMySQL);
        if (valueNonMySQL.equals("NULL")) {
            return false;
        }
        float floatHBase = Float.parseFloat(valueNonMySQL);
        //if (nrDigits > 0) {
        //    floatHBase = Float.parseFloat(valueMySQL.split(".")[0] + valueMySQL.split(".")[1].substring(0, nrDigits));
        //} else {
        //    floatHBase = Float.parseFloat(valueNonMySQL);
        //}

        return abs(floatHBase - floatMySQL) < smallValue;
    }

    Boolean compareChar(String valueMySQL, String valueNonMySQL) {
        // TODO: encoding check
        return valueMySQL.equals(valueNonMySQL);
    }

    Boolean compareText(String valueMySQL, String valueNonMySQL) {
        if (valueMySQL == null) {
            return valueNonMySQL.equals("NULL");
        }
        return valueMySQL.equals(valueNonMySQL);
    }

    Boolean compareTimestamp(String valueMySQL, String valueNonMySQL) {
        // TODO: timestamp comparator;
        return true;
    }

    Boolean compareDate(String valueMySQL, String valueNonMySQL) {
        // TODO: date comparator;
        // DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //try {
        //    Date dateTime = format.parse(valueMySQL.substring(0, -2));
        //    System.out.println("bijiao: " + dateTime);
        //    return true; // timestampMySQL.equals(valueNonMySQL);
        //} catch (ParseException pa) {
        //    pa.printStackTrace();;
        //}
        return true;
    }

    Boolean typeHelper(String type, String typename) {
        if (type.length() >= typename.length() && type.substring(0, typename.length()).equals(typename)) {
            return true;
        }
        return false;
    }

    Boolean comparisonHelper(String type, String valueMySQL, String valueNonMySQL) {
        if (typeHelper(type, "tinyint") || typeHelper(type, "int") || typeHelper(type, "char")
                || typeHelper(type, "varchar") || typeHelper(type, "double") || typeHelper(type, "bigint")) {
            return compareGeneral(valueMySQL, valueNonMySQL);
        }
        if (typeHelper(type, "float")) {
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
