package com.taobao.yugong.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2017-02-23
 * Time: 18:01
 */
public class UserIndexs {

    private String tableName;
    private Map<String, UserIndexItem> indexes = new HashMap<String, UserIndexItem>();

    public Map<String, UserIndexItem> getIndexes() {
        return indexes;
    }

    public UserIndexs setIndexes(Map<String, UserIndexItem> indexes) {
        this.indexes = indexes;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public UserIndexs setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
}
