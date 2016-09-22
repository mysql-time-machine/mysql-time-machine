package com.booking.sql;

class Test {
    public static void main(String[] args) {
        MySQLDataType dt = DataTypeParser.apply("INTEGER(10, 2) UNSIGNED ZEROFILL CHARACTER SET \"utf-8\" COLLATE \'latin1_bin\'");
        System.out.println(dt);
        System.out.println(dt.typename());
        System.out.println(dt.precision().get());
        System.out.println(dt.precision().get().precision());
        System.out.println(dt.precision().get().scale().get());
        System.out.println(dt.qualifiers());
        System.out.println(dt.attributes());
    }
}
