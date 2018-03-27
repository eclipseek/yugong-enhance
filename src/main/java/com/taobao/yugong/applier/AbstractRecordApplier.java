package com.taobao.yugong.applier;

import java.sql.*;
import java.util.*;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.UserIndexItem;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.Keywords;
import com.taobao.yugong.common.utils.TypeMapping;
import com.taobao.yugong.exception.YuGongException;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import javax.sql.DataSource;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public abstract class AbstractRecordApplier extends AbstractYuGongLifeCycle implements RecordApplier {
    private static Object mutex = new Object();
    private final Logger logger          = LoggerFactory.getLogger(AbstractRecordApplier.class);

    public static class TableSqlUnit {

        public String               applierSql;
        public Map<String, Integer> applierIndexs;
    }

    protected Integer getIndex(final Map<String, Integer> indexs, ColumnValue cv) {
        return getIndex(indexs, cv, false);
    }

    protected Integer getIndex(final Map<String, Integer> indexs, ColumnValue cv, boolean notExistReturnNull) {
        Integer result = indexs.get(cv.getColumn().getName());
        if (result == null && !notExistReturnNull) {
            throw new YuGongException("not found column[" + cv.getColumn().getName() + "] in record");
        } else {
            return result;
        }
    }

    /**
     * 检查下是否存在必要的字段
     */
    protected void checkColumns(Table meta, Map<String, Integer> indexs) {
        Set<String> idx = new HashSet<String>();
        for (ColumnMeta column : meta.getColumns()) {
            idx.add(column.getName());
        }

        for (ColumnMeta column : meta.getPrimaryKeys()) {
            idx.add(column.getName());
        }

        for (String key : indexs.keySet()) {
            if (!idx.contains(key)) {
                throw new YuGongException("not found column[" + key + "] in target db");
            }
        }
    }

    protected void checkColumnsWithoutPk(Table meta, Map<String, Integer> indexs) {
        Set<String> idx = new HashSet<String>();
        for (ColumnMeta column : meta.getColumns()) {
            idx.add(column.getName());
        }

        for (String key : indexs.keySet()) {
            if (!idx.contains(key)) {
                throw new YuGongException("not found column[" + key + "] in target db");
            }
        }
    }

    /**
     * 获取主键字段信息
     */
    protected List<ColumnMeta> getPrimaryMetas(Record record) {
        List<ColumnMeta> result = Lists.newArrayList();
        for (ColumnValue col : record.getPrimaryKeys()) {
            result.add(col.getColumn());
        }
        return result;
    }

    /**
     * 获取普通列字段信息
     */
    protected List<ColumnMeta> getColumnMetas(Record record) {
        List<ColumnMeta> result = Lists.newArrayList();
        for (ColumnValue col : record.getColumns()) {
            result.add(col.getColumn());
        }
        return result;
    }

    /**
     * 获取主键字段信息
     */
    protected String[] getPrimaryNames(Record record) {
        String[] result = new String[record.getPrimaryKeys().size()];
        int i = 0;
        for (ColumnValue col : record.getPrimaryKeys()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }

    /**
     * 获取主键字段信息，从Table元数据中获取。因为物化视图由with primary 改为with (shardkey)
     *
     * @param
     * @return
     */
    protected String[] getPrimaryNames(Table tableMeta) {
        String[] result = new String[tableMeta.getPrimaryKeys().size()];
        int i = 0;
        for (ColumnMeta col : tableMeta.getPrimaryKeys()) {
            result[i++] = col.getName();
        }
        return result;
    }

    /**
     * 获取普通列字段信息
     */
    protected String[] getColumnNames(Record record) {
        String[] result = new String[record.getColumns().size()];
        int i = 0;
        for (ColumnValue col : record.getColumns()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }


    private Map<String, Map<String, Boolean>> tableExistCache = new HashMap<String, Map<String, Boolean>>();
    protected boolean checkIfTableExist(Table tableMeta) {
        boolean exist = false;


        return exist;
    }

    public boolean isTableExist(YuGongContext context) {
        boolean exist = false;

        final DataSource ds = context.getTargetDs();
        Table table = context.getTableMeta();

        // 查询表是否已经存在，如已存在不创建。
        final String tableName = table.getName().toLowerCase();
        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
            exist = (Boolean) jdbcTemplate.execute(new ConnectionCallback() {
                public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                    boolean exist = false;
                    String database = con.getCatalog();

                    // 接入 dadb
                    if (((DruidDataSource) ds).getUrl().contains("dadb")) {
                        String sql = "select * from " + tableName;
                        try {
                          con.prepareStatement(sql).executeQuery();
                          exist = true;
                        } catch (Exception e) {
                          // 表不存在
                          if (e.toString().toLowerCase().contains("doesn't exist")) {
                              exist = false;
                          } else {
                            logger.error("ERROR: ", e);
                          }
                        }
                    } else {
                      ResultSet rs = con.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "'").executeQuery();
                      while (rs.next()) {
                        if(rs.getString(1).toLowerCase().equals(tableName)) {
                          logger.info("table [{}]  exist, exit create.", tableName);
                          exist = true;
                          break;
                        }
                      }
                    }
                  return exist;
                }
            });
        } catch (Exception e) {
            logger.error("error: ", e);
        }

        return exist;
    }

    private ThreadLocal<Integer> recursion = new ThreadLocal<Integer>() {
        public Integer initialValue() {
            return 0;
        }
    };

    /**
     * 创建表。
     * @return
     */
    public boolean createTable(YuGongContext context) {
        DataSource ds = context.getTargetDs();
        Table table = context.getTableMeta();
        final String tableName = table.getName().toLowerCase();
        List<ColumnMeta> columns = table.getColumnsWithPrimary();
        // 每个列有自己的 order（表示顺序，可以从oracle 元数据信息中查出），依据 order 排个序
        Collections.sort(columns);

        logger.info("begin create table [{}], retry times = {}", tableName, recursion.get());

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(tableName + "(");

        // 用户索引信息。
        UserIndexList<UserIndexItem> userIndexItems = new UserIndexList<UserIndexItem>();
        // 第一次不用加载索引
        if (recursion.get() > 0) {
            if (!indexLoaded) {
                loadUserIndexes(context.getSourceDs());
            }
            for (List<UserIndexItem> items: allTableIndexes.get(tableName).values()) {
                userIndexItems.addAll(items);
            }
        }

        // 拼接列
        columnsCreateString(table, userIndexItems, sb);

        checkRowId(table, context, sb);

        sb.delete(sb.length()-1, sb.length());
        sb.append("\n)");
        final String sql = sb.toString();
        logger.info("create table sql: \n" + sql);

        int recursion_temp = recursion.get();
        // 执行建表语句
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        Object rtn = jdbcTemplate.execute(sql, new PreparedStatementCallback() {
            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                try {
                    ps.execute(sql);
                    logger.info("table [{}] create success!", tableName);
                } catch (SQLException e) {
                    logger.error("table [{}] create failed! reason: {}", tableName, e.getMessage());
                    int errorCode = e.getErrorCode();
                    if (errorCode == 1118) {
                        recursion.set(recursion.get() + 1);
                    } else {
                        logger.error("ERROR: ", e);
                    }

                    return null;
                }

                return new Object();
            }
        });

        // 创建失败，递归一次。
        if (recursion.get() > recursion_temp) {
            rtn = createTable(context);
        }

        if (recursion.get() > 0) {
            recursion.set(recursion.get() - 1);
        }

        logger.info("end create table [{}], retry times = {}", tableName, recursion.get());

        // 返回是否创建成功
        return rtn == null? false : true;
    }

    /**
     * 拼接建表语句中的 列部分。
     * @param table 表信息
     * @param userIndexItems 索引信息
     * @param dest 目标串
     * @return void
     */
    private void columnsCreateString(Table table, UserIndexList<UserIndexItem> userIndexItems, StringBuilder dest) {

        String tableName = table.getName().toLowerCase();
        List<ColumnMeta> columnMetas = table.getColumnsWithPrimary();

        // 所有列
        for(ColumnMeta cm : columnMetas) {
            int length = cm.getData_length();
            int type = cm.getType();
            String name = cm.getName();
            if (Keywords.contains(name)) {
                name += "_MYSQL";
            }

            dest.append("\n");
            dest.append(name + " ");

            boolean appendLength = true;
            // 需要将部分 varchar 字段转换成 TEXT/BLOB，
            if (recursion.get() > 0 && (type == Types.VARCHAR)) {
                // 优先将所有不在 index 中的字段，都转换成 TEXT/BLOB
                if (recursion.get() == 1) {
                    // 字段不在 index 列表中，将其替换成 TEXT/BLOB
                    if (!userIndexItems.contains(name)) {
                        dest.append("TEXT");
                        appendLength = false;
                        logger.info("table [{}].{} is not index, type change from varchar to text. ", tableName, name);
                    } else {
                        dest.append(TypeMapping.get(type));
                    }
                }
                // 如果不是 index 的字段转换成 TEXT/BLOB 类型后，创建表还是存在 error= 1118 类型 sql 异常，
                // 再考虑将普通类型的（非 primary key, 非 unique ）索引字段，转换成 TEXT/BLOB 类型。
                else if (recursion.get() == 2) {
                    UserIndexList<UserIndexItem> normalIndex = userIndexItems.getIndex(name, null);
                    if(normalIndex.contains(name)) {
                        dest.append("TEXT");
                        appendLength = false;
                        logger.warn("table [{}].{} is normal index, type change from varchar to text! ",tableName, name);
                    } else {
                        dest.append(TypeMapping.get(type));
                    }
                }

                // 如果还是报 1118 类型 sql 异常，再考虑将 unique 类型索引，转换成 TEXT/BLOB 类型。
                else if (recursion.get() == 3) {
                    UserIndexList<UserIndexItem> uniqueIndex = userIndexItems.getIndex(name, "u");
                    if(uniqueIndex.contains(name)) {
                        dest.append("TEXT");
                        appendLength = false;
                        logger.warn("table [{}].{} is unique index, type change from varchar to text! ",tableName, name);
                    } else {
                        dest.append(TypeMapping.get(type));
                    }
                }

                // 如果以上1，2，3 情况下 varchar 类型都转换 TEXT/BLOB 后，还是报 1118 sql 异常，
                // 最后考虑将 primary key 的索引字段，转换成 TEXT/BLOB 类型。
                // 这种情况是最坏的打算，所以放到最低优先级，如果发生这种情况，说明在 oracle 到 mysql 的表结构迁移存在兼容问题，
                // 且没有妥协方案，此时应考虑重新设计表。
                else if (recursion.get() == 4) {
                    UserIndexList<UserIndexItem> primaryIndex = userIndexItems.getIndex(name, "p");
                    if(primaryIndex.contains(name)) {
                        dest.append("TEXT");
                        appendLength = false;
                        logger.warn("table [{}].{} is primary index, type change from varchar to text! ",tableName, name);
                    } else {
                        dest.append(TypeMapping.get(type));
                    }
                }

                else {
                    logger.warn("recursion > 4, change [{}] all varchar field to text!", tableName);
                    dest.append("TEXT");
                    appendLength = false;
                }
            } else {
                dest.append(TypeMapping.get(type));
            }

            if(type == Types.DECIMAL || type == Types.CHAR || (type == Types.VARCHAR)) {
                if (appendLength) {
                    dest.append("(" + length + ")");
                }
            }

            if (type == Types.CHAR || type == Types.VARCHAR) {
                dest.append(" BINARY");
            }

            dest.append(",");
        }
    }

    /**
     * 检查是否要用 oracle 的 rowid 作为主键。
     * @param table
     * @param context
     * @param dest
     * @return void
     */
    private void checkRowId(Table table, YuGongContext context, StringBuilder dest) {
        // 如果有主键了，就不检查了。
        if (table.getPrimaryKeys().size() > 0) {
            return;
        }

        // 配置文件声明了需要添加
        if (!context.isAddPrimaryKey()) {
            return;
        }

        String rowid = "\nROWID VARCHAR(50) BINARY NOT NULL,\nPRIMARY KEY (ROWID),";
        dest.append(rowid);
    }

    // tableName, indexName, UserIndex
    private static volatile Map<String, Map<String, List<UserIndexItem>>> allTableIndexes = new CaseInsensitiveMap();
    private static boolean indexLoaded = false;
    public void createIndex(YuGongContext context) {
        DataSource ds = context.getTargetDs();
        if (!indexLoaded) {
            loadUserIndexes(context.getSourceDs());
        }

        Table table = context.getTableMeta();
        String tableName = table.getName();
        List<ColumnMeta> columns = table.getColumnsWithPrimary();
        Map<String, List<UserIndexItem>> tableIndexes = allTableIndexes.get(tableName);
        if (tableIndexes == null) {
            return;
        }

        for (String indexName : tableIndexes.keySet()) {
            List<UserIndexItem> indexes = tableIndexes.get(indexName);
            if (indexes == null || indexes.isEmpty()) {
                continue;
            }

            String constraint_type = indexes.get(0).getConstraintType();
            String uniqueness = indexes.get(0).getUniqueness();
            StringBuilder sb = new StringBuilder();
            if (constraint_type == null || constraint_type.isEmpty()) {
                if (uniqueness.equalsIgnoreCase("nonunique")) {
                    sb.append("create index " + indexName + " on " + tableName + "(");
                }
                // 遇到一种情况，uniqueue 索引的 constraint_type 值为空，所以这里再通过 unique 判断下。
                if (uniqueness.equalsIgnoreCase("unique")) {
                    sb.append("alter table " + tableName + "  add constraint " + indexName + " unique (");
                }
            } else if (constraint_type.equalsIgnoreCase("u")) {
                sb.append("alter table " + tableName + "  add constraint " + indexName + " unique (");
            } else if (constraint_type.equalsIgnoreCase("p")) {
                sb.append("alter table " + tableName + "  add constraint " + indexName + " primary key (");
            }

            for(UserIndexItem uii : indexes) {
                sb.append(uii.getColumnName());
                sb.append(",");
            }
            sb.delete(sb.length()-1, sb.length());
            sb.append(")");

            final String sql = sb.toString();
            JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
            Object rtn = jdbcTemplate.execute(sql, new PreparedStatementCallback() {
                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    try {
                        ps.execute(sql);
                        logger.error("index create sucess! sql: {}", sql);
                    } catch (SQLException e) {
                        logger.error("index create failed! sql: {}", sql);
                        logger.error("error: ", e);
                        return null;
                    }

                    return new Object();
                }
            });
        }
    }

    /**
     * 从源库查找有哪些 index
     * @param ds    源库
     * @return void
     */
    private void loadUserIndexes(DataSource ds) {
        synchronized(mutex) {
            if (!indexLoaded) {
                JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);

                final String sql =
                        "select t.*, uc.constraint_type\n" +
                                "  from (select ui.table_name,\n" +
                                "               ui.index_name,\n" +
                                "               ui.index_type,\n" +
                                "               ui.uniqueness,\n" +
                                "               uic.column_name,\n" +
                                "               uic.column_position\n" +
                                "          from user_indexes ui, user_ind_columns uic\n" +
                                "         where ui.index_name = uic.index_name) t\n" +
                                "  left join user_constraints uc\n" +
                                "    on t.index_name = uc.constraint_name";
                indexLoaded = (Boolean)jdbcTemplate.execute(sql, new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        boolean rtn = false;
                        try {
                            ResultSet rs = ps.executeQuery();
                            while (rs.next()) {
                                String tableName = rs.getString("TABLE_NAME");
                                String indexName = rs.getString("INDEX_NAME");
                                String indexType = rs.getString("INDEX_TYPE");
                                String uniqueness = rs.getString("UNIQUENESS");
                                String constraintType = rs.getString("CONSTRAINT_TYPE");
                                String columnName = rs.getString("COLUMN_NAME");
                                int columnPosition = rs.getInt("COLUMN_POSITION");

                                Map<String, List<UserIndexItem>> oneTableIndexes = allTableIndexes.get(tableName);
                                if (null == oneTableIndexes) {
                                    oneTableIndexes = new HashMap<String, List<UserIndexItem>>();
                                    allTableIndexes.put(tableName, oneTableIndexes);
                                }

                                List<UserIndexItem> indexes = oneTableIndexes.get(indexName);
                                if (null == indexes) {
                                    indexes = new ArrayList<UserIndexItem>();
                                    oneTableIndexes.put(indexName, indexes);
                                }

                                UserIndexItem userIndexItem = new UserIndexItem();
                                userIndexItem.setTableName(tableName);
                                userIndexItem.setIndexName(indexName);
                                userIndexItem.setIndexType(indexType);
                                userIndexItem.setUniqueness(uniqueness);
                                userIndexItem.setConstraintType(constraintType);
                                userIndexItem.setColumnName(columnName);
                                userIndexItem.setColumnPosition(columnPosition);
                                indexes.add(userIndexItem);
                                Collections.sort(indexes);
                            }
                            rtn = true;
                        } catch (Exception e) {
                            logger.error("ERROR: ", e);
                            rtn = false;
                            allTableIndexes.clear();
                        } finally {
                            return rtn;
                        }
                    }
                });
            } // end of if (!indexLoaded) {
        } // end of synchronized(

        logger.debug("user index loaded: " + allTableIndexes);
    }

    private class UserIndexList<E> extends ArrayList<E> {
        public boolean contains(Object o) {
            if (o == null) {
                return false;
            }

            if(o instanceof UserIndexItem) {
                return super.contains(o);
            } else if (o instanceof String) {   // 列名相同，也认为包含
                boolean contains = false;
                String columnName = (String)o;
                for (int i = 0; i < super.size(); i++) {
                    UserIndexItem uii = (UserIndexItem)super.get(i);
                    if (uii.getColumnName().equalsIgnoreCase(columnName)) {
                        contains = true;
                        break;
                    }
                }
                return contains;
            } else {
                return false;
            }
        }

        // 获取指定列上附加的 type 类型的 UserIndexItem 对象
        public UserIndexList<UserIndexItem> getIndex(String columnName, String type) {
            UserIndexList<UserIndexItem> list = new UserIndexList<UserIndexItem>();
            for (int i = 0; i < super.size(); i++) {
                UserIndexItem uii = (UserIndexItem)super.get(i);
                if (!uii.getColumnName().equalsIgnoreCase(columnName)) {
                    continue;
                }

                // 获取普通索引（非 primary，非 unique）
                if (type == null || type.isEmpty()) {
                    // 普通的索引，constraint_type 是空的。
                    if (uii.getConstraintType() == null || uii.getConstraintType().isEmpty()) {
                        list.add(uii);
                    }
                } else {    // 获取 primary 或 unique 类型索引
                    if (uii.getConstraintType() == null || uii.getConstraintType().isEmpty()) {
                        continue;
                    }

                    if (uii.getConstraintType().equalsIgnoreCase(type)) {
                        list.add(uii);
                    }
                }
            }

            return list;
        }
    }
}
