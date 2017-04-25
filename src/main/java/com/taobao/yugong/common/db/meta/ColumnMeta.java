package com.taobao.yugong.common.db.meta;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.yugong.common.utils.YuGongToStringStyle;

import java.util.Comparator;

/**
 * 代表一个字段的信息
 * 
 * @author agapple 2013-9-3 下午2:46:32
 * @since 3.0.0
 */
public class ColumnMeta implements Comparable {

    private String name;
    private int    type;

    // 字段长度，用于在 mysql 中创建表时使用。
    private int data_length;

    private int order;

    public ColumnMeta(String columnName, int columnType, int data_length, int order){
        this.name = StringUtils.upperCase(columnName);// 统一为大写
        this.type = columnType;
        this.data_length = data_length;
        this.order = order;
    }

    public ColumnMeta(String columnName, int columnType){
        this.name = StringUtils.upperCase(columnName);// 统一为大写
        this.type = columnType;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public void setName(String name) {
        this.name = StringUtils.upperCase(name);
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getData_length() {
        return data_length;
    }

    public ColumnMeta setData_length(int data_length) {
        this.data_length = data_length;
        return this;
    }

    public ColumnMeta clone() {
        return new ColumnMeta(this.name, this.type, this.data_length, order);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, YuGongToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnMeta other = (ColumnMeta) obj;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (type != other.type) return false;
        return true;
    }

    @Override
    public int compareTo(Object o) {
        return this.order - ((ColumnMeta)o).order;
    }
}
