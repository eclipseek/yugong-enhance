package com.taobao.yugong.common.db.sql;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class SqlTemplates {

    public static SqlTemplate       COMMON = new SqlTemplate();
    public static MysqlSqlTemplate  MYSQL  = new MysqlSqlTemplate();
    public static DADBSqlTemplate DADB  = new DADBSqlTemplate();
    public static SunDBSqlTemplate SUNDB  = new SunDBSqlTemplate();
    public static OracleSqlTemplate ORACLE = new OracleSqlTemplate();
}
