package com.taobao.yugong.common.model;

/**
 * @author agapple 2011-9-2 上午11:36:21
 */
public enum DbType {

    /** mysql DB */
    MYSQL("com.mysql.jdbc.Driver"),
    /** drds DB */
    DRDS("com.mysql.jdbc.Driver"),
    /** oracle DB */
    ORACLE("oracle.jdbc.driver.OracleDriver"),

    // DADB("com.asiainfo.dadb.client.jdbc.Driver"),
    DADB("com.ai.aif.dadb.jdbc.DadbDriver"),

    SUNDB("sunje.sundb.jdbc.SundbDriver");

    private String driver;

    DbType(String driver){
        this.driver = driver;
    }

    public String getDriver() {
        return driver;
    }

    public boolean isMysql() {
        return this.equals(DbType.MYSQL);
    }

    public boolean isDRDS() {
        return this.equals(DbType.DRDS);
    }

    public boolean isOracle() {
        return this.equals(DbType.ORACLE);
    }

    public boolean isDADB() {
        return this.equals(DbType.DADB);
    }

    public boolean isSunDB() {
        return this.equals(DbType.SUNDB);
    }

}
