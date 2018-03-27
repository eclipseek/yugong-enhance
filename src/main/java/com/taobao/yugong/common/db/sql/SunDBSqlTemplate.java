package com.taobao.yugong.common.db.sql;


import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.model.DbType;

/**
 * mysql特定的sql构造
 *
 * @author agapple 2013-9-10 下午6:11:16
 * @since 3.0.0
 */
public class SunDBSqlTemplate extends SqlTemplate {

    public String getMergeSql(String schemaName, String tableName, String[] pkNames, String[] colNames,
                              boolean mergeUpdatePk) {
        String sql = "";
        // 暂不支持 qcubic (sundb) 存在则更新语法格式
        boolean support = false;
        if (!support) {
            sql = super.getInsertSql(schemaName, tableName, pkNames, colNames);
        } else {
            // 按照 qcubic 语法格式拼接 sql 语句。
        }

        return sql;
    }

    public String getInsertSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
        return super.getInsertSql(schemaName, tableName, pkNames, columnNames);
    }

    public String getInsertNomalSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(makeFullName(schemaName, tableName)).append("(");
        String[] allColumns = buildAllColumns(pkNames, columnNames);

        int size = allColumns.length;
        for (int i = 0; i < size; i++) {
            sql.append(getColumnName(allColumns[i])).append(splitCommea(size, i));
        }

        sql.append(") values (");
        makeColumnQuestions(sql, allColumns);
        sql.append(")");
        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
    }

    protected String getColumnName(String columName) {
        return columName;
        //return "`" + columName + "`";
    }

    protected String getColumnName(ColumnMeta column) {
        return "`" + column.getName() + "`";
    }

}
