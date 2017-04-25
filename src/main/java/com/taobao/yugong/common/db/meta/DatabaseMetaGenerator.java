package com.taobao.yugong.common.db.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2017-02-07
 * Time: 10:27
 */
public class DatabaseMetaGenerator {
    private static final Logger logger               = LoggerFactory.getLogger(DatabaseMetaGenerator.class);
    private static String getAllTables = "select * from all_tables WHERE owner=";

    public static List getAllTableNames(final DataSource dataSource, final String owner) {
        logger.debug("NO white table config, sync all table for user: " + owner);

        final List tables = new ArrayList();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return (List) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement ps = conn.prepareStatement(getAllTables + "'" + owner.toUpperCase() + "'");

                ResultSet rs = ps.executeQuery();
                int i = 0;
                while (rs.next()) {
                    i++;
                    // for test!!
//                    if(i <= 250 || i > 400) {
//                        continue;
//                    }


                    String tableName = rs.getString("TABLE_NAME");

                    int index = tableName.indexOf("_2016");
                    if (index == -1) {
                        index = tableName.indexOf("_2017");
                    }
                    if (index == -1) {
                        index = tableName.indexOf("_2018");
                    }

                    if (index == -1) {
                        index = tableName.indexOf("_47");
                    }
                    if (index == -1) {
                        index = tableName.indexOf("_48");
                    }

                    if (index > -1) {
                        continue;
                    }
                    tables.add(owner + "." + tableName);
                }

                rs.close();
                return tables;
            }
        });
    }
}
