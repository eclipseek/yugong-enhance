package com.taobao.yugong.common.model;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2017-02-23
 * Time: 22:23
 */
public class UserIndexItem implements Comparable{
    private String tableName;
    private String indexName;
    private String indexType;
    private String uniqueness;
    private String constraint_type;
    private String columnName;
    private int columnPosition;


    public String getTableName() {
        return tableName;
    }

    public UserIndexItem setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getIndexName() {
        return indexName;
    }

    public UserIndexItem setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public String getIndexType() {
        return indexType;
    }

    public UserIndexItem setIndexType(String indexType) {
        this.indexType = indexType;
        return this;
    }

    public String getUniqueness() {
        return uniqueness;
    }

    public UserIndexItem setUniqueness(String uniqueness) {
        this.uniqueness = uniqueness;
        return this;
    }

    public String getConstraintType() {
        return constraint_type;
    }

    public UserIndexItem setConstraintType(String constraint_type) {
        this.constraint_type = constraint_type;
        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    public UserIndexItem setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public int getColumnPosition() {
        return columnPosition;
    }

    public UserIndexItem setColumnPosition(int columnPosition) {
        this.columnPosition = columnPosition;
        return this;
    }


    @Override
    public int compareTo(Object o) {
        return this.getColumnPosition() - ((UserIndexItem)o).getColumnPosition();
    }

    public String toString() {
        return "\ntableName = " + tableName + ",\tindexName = " + indexName + ",\tcolumnName = " + columnName + ",\tcolumnPosition = " + columnPosition + ",\tconstraintType = " + constraint_type;
    }
}
