package com.booking.validator.data.storage;

import com.booking.validator.data.Data;

/**
 * Created by psalimov on 9/5/16.
 */
public interface Value {

    Data getData();

    /*

        BIT(M) ->   string of "0","1" of lenght M, biggest bit comes first (big endian?)

        TINYINT -> (signed/unsigned) maps to decimal string representation

        SMALLINT ->



MEDIUMINT	3 bytes
INT, INTEGER	4 bytes
BIGINT	8 bytes
FLOAT(p)	4 bytes if 0 <= p <= 24, 8 bytes if 25 <= p <= 53
FLOAT	4 bytes
DOUBLE [PRECISION], REAL	8 bytes
DECIMAL(M,D), NUMERIC(M,D)	Varies; see following discussion





     */


}
