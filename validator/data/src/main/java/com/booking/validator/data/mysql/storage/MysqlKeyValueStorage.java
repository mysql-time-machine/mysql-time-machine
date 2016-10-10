package com.booking.validator.data.mysql.storage;

import com.booking.validator.data.storage.KeyValueStorage;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by psalimov on 9/5/16.
 */
public class MysqlKeyValueStorage implements KeyValueStorage<MysqlKey, MysqlValue> {



    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlKeyValueStorage.class);

    private final BasicDataSource dataSource;

    public MysqlKeyValueStorage(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }


    @Override
    public MysqlValue get(MysqlKey key) {

        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(getSelectSql());
        ){



        } catch (SQLException e) {
            LOGGER.error("SQL exception: ", e);
        }


        return null;
    }

    private String getSelectSql(){
        return null;
    }
}
