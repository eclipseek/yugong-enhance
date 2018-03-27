package com.taobao.yugong.applier;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MigrateMap;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.db.sql.SqlTemplates;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 全量同步appiler
 * 
 * <pre>
 * 1. 行记录同步，目标库存在则更新，没有则插入
 * </pre>
 * 
 * @author agapple 2013-9-23 下午5:27:02
 */
public class FullRecordApplier extends AbstractRecordApplier {

    protected static final Logger             logger   = LoggerFactory.getLogger(FullRecordApplier.class);
    protected Map<List<String>, TableSqlUnit> applierSqlCache;
    protected YuGongContext                   context;
    protected DbType                          dbType;
    protected boolean                         useMerge = false;

    public FullRecordApplier(YuGongContext context){
        this.context = context;
    }

    public void start() {
        super.start();
        dbType = YuGongUtils.judgeDbType(context.getTargetDs());
        applierSqlCache = MigrateMap.makeMap();
    }

    public void stop() {
        super.stop();
    }

    /**
     * default batch insert
     */
    public void apply(List<Record> records) throws YuGongException {
        // no one,just return
        if (YuGongUtils.isEmpty(records)) {
            return;
        }

        doApply(records);
    }


    protected void doApply(List<Record> records) {
        Map<List<String>, List<Record>> buckets = MigrateMap.makeComputingMap(new Function<List<String>, List<Record>>() {

            public List<Record> apply(List<String> names) {
                return Lists.newArrayList();
            }
        });

        // 根据目标库的不同，划分为多个bucket
        for (Record record : records) {
            buckets.get(Arrays.asList(record.getSchemaName(), record.getTableName())).add(record);
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        for (final List<Record> batchRecords : buckets.values()) {
            TableSqlUnit sqlUnit = getSqlUnit(batchRecords.get(0));
            if (context.isBatchApply()) {
                if(context.isApplierWithTransaction()) {
                    // 批量执行带事务
                    applierByBatchWithTransaction(jdbcTemplate, batchRecords, sqlUnit);
                } else {
                    // 批量执行不带事务
                    applierByBatchWithoutTransaction(jdbcTemplate, batchRecords, sqlUnit);
                }

            } else {
                if(context.isApplierWithTransaction()) {
                    // 单个执行带事务
                    applyOneByOneWithTransaction(jdbcTemplate, batchRecords, sqlUnit);
                } else {
                    // 单个执行不带事务
                    applyOneByOneWithoutTransaction(jdbcTemplate, batchRecords, sqlUnit);
                }
            }
        }
    }


    /**
     * batch处理支持。带事务。
     */
    protected void applierByBatchWithTransaction(JdbcTemplate jdbcTemplate, final List<Record> batchRecords, TableSqlUnit sqlUnit) {
        boolean redoOneByOne = false;

        // 加入事务控制
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW); //  事务隔离级别
        final DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(
                jdbcTemplate.getDataSource());
        final TransactionStatus status = transactionManager.getTransaction(def);
        transactionManager.setRollbackOnCommitFailure(true);

        try {
            final Map<String, Integer> indexs = sqlUnit.applierIndexs;
            jdbcTemplate.execute(sqlUnit.applierSql, new PreparedStatementCallback() {

                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    for (Record record : batchRecords) {
                        // 先加字段，后加主键
                        List<ColumnValue> cvs = record.getColumns();
                        for (ColumnValue cv : cvs) {
                            ps.setObject(getIndex(indexs, cv), cv.getValue(), cv.getColumn().getType());
                        }

                        // 添加主键
                        List<ColumnValue> pks = record.getPrimaryKeys();
                        String pkValues = "";
                        for (ColumnValue pk : pks) {
                            ps.setObject(getIndex(indexs, pk), pk.getValue(), pk.getColumn().getType());
                            pkValues += pk.getValue();
                        }

                        ps.addBatch();
                    }

                    int[]  ret = ps.executeBatch();
                    transactionManager.commit(status);

                    return ret;
                }
            });
        } catch (Exception e) {
            // 批量提交失败，尝试逐个提交
            redoOneByOne = true;
            transactionManager.rollback(status);
        }

        // batch cannot pass the duplicate entry exception,so
        // if executeBatch throw exception,rollback it, and
        // redo it one by one
        if (redoOneByOne) {
            if (context.isApplierWithTransaction()) {
                applyOneByOneWithTransaction(jdbcTemplate, batchRecords, sqlUnit);
            } else {
                applyOneByOneWithoutTransaction(jdbcTemplate, batchRecords, sqlUnit);
            }
        }
    }

    /**
     * batch处理支持。不带事务
     */
    protected void applierByBatchWithoutTransaction(JdbcTemplate jdbcTemplate, final List<Record> batchRecords, TableSqlUnit sqlUnit) {
        boolean redoOneByOne = false;
        try {
            final Map<String, Integer> indexs = sqlUnit.applierIndexs;
            jdbcTemplate.execute(sqlUnit.applierSql, new PreparedStatementCallback() {

                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    for (Record record : batchRecords) {
                        // 先加字段，后加主键
                        List<ColumnValue> cvs = record.getColumns();
                        for (ColumnValue cv : cvs) {
                            ps.setObject(getIndex(indexs, cv), cv.getValue(), cv.getColumn().getType());
                        }

                        // 添加主键
                        List<ColumnValue> pks = record.getPrimaryKeys();
                        for (ColumnValue pk : pks) {
                            ps.setObject(getIndex(indexs, pk), pk.getValue(), pk.getColumn().getType());
                        }

                        ps.addBatch();
                    }

                    ps.executeBatch();
                    return null;
                }
            });
        } catch (Exception e) {
            // catch the biggest exception,no matter how, rollback it;
            redoOneByOne = true;
            // conn.rollback();
        }

        // batch cannot pass the duplicate entry exception,so
        // if executeBatch throw exception,rollback it, and
        // redo it one by one
        if (redoOneByOne) {
            if (context.isApplierWithTransaction()) {
                applyOneByOneWithTransaction(jdbcTemplate, batchRecords, sqlUnit);
            } else {
                applyOneByOneWithoutTransaction(jdbcTemplate, batchRecords, sqlUnit);
            }
        }
    }

    /**
     * 一条条记录串行处理。不带事务。
     */
    protected void applyOneByOneWithoutTransaction(JdbcTemplate jdbcTemplate, final List<Record> records, TableSqlUnit sqlUnit) {
        final Map<String, Integer> indexs = sqlUnit.applierIndexs;
        jdbcTemplate.execute(sqlUnit.applierSql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                for (Record record : records) {
                    setPrepareStatement(ps, record, indexs);
                    try {
                        ps.execute();
                    } catch (SQLException e) {
                        handleException(e, record);
                    }
                }
                return null;
            }
        });

    }


    /**
     * 一条条记录串行处理。 带事务。
     */
    protected void applyOneByOneWithTransaction(JdbcTemplate jdbcTemplate, final List<Record> records, TableSqlUnit sqlUnit) {
        final Map<String, Integer> indexs = sqlUnit.applierIndexs;
        for (final Record record : records) {
            // 加入事务控制
            final DefaultTransactionDefinition def = new DefaultTransactionDefinition();
            def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW); //  事务隔离级别
            final DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(jdbcTemplate.getDataSource());
            final TransactionStatus status = transactionManager.getTransaction(def);
            transactionManager.setRollbackOnCommitFailure(true);

            jdbcTemplate.execute(sqlUnit.applierSql, new PreparedStatementCallback() {
                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    setPrepareStatement(ps, record, indexs);
                    try {
                        ps.execute();
                        transactionManager.commit(status);
                    } catch (SQLException e) {
                        handleException(e, record);
                        transactionManager.rollback(status);
                    }

                    return null;
                }
            });
        }
    }

    private void setPrepareStatement(PreparedStatement ps, Record record, Map<String, Integer> indexs) throws SQLException {
        List<ColumnValue> pks = record.getPrimaryKeys();
        // 先加字段，后加主键
        List<ColumnValue> cvs = record.getColumns();
        for (ColumnValue cv : cvs) {
            ps.setObject(getIndex(indexs, cv), cv.getValue(), cv.getColumn().getType());
        }

        // 添加主键
        for (ColumnValue pk : pks) {
            ps.setObject(getIndex(indexs, pk), pk.getValue(), pk.getColumn().getType());
        }
    }

    private void handleException(Exception e, Record record) throws SQLException {
        if (context.isSkipApplierException()) {
            logger.error("skiped record data : " + record.toString(), e);
        } else {
            if (e.getMessage().contains("Duplicate entry")
                    || e.getMessage().startsWith("ORA-00001: 违反唯一约束条件")) {
                logger.error("skiped record data ,maybe transfer before,just continue:"
                        + record.toString());
            } else {
                throw new SQLException("failed Record Data : " + record.toString(), e);
            }
        }
    }

    /**
     * 基于当前记录生成sqlUnit
     */
    protected TableSqlUnit getSqlUnit(Record record) {
        List<String> names = Arrays.asList(record.getSchemaName(), record.getTableName());
        TableSqlUnit sqlUnit = applierSqlCache.get(names);
        if (sqlUnit == null) {
            synchronized (names) {
                sqlUnit = applierSqlCache.get(names);
                if (sqlUnit == null) { // double-check
                    sqlUnit = new TableSqlUnit();
                    String applierSql = null;
                    Table meta = TableMetaGenerator.getTableMeta(context.getTargetDs(),
                        context.isIgnoreSchema() ? null : names.get(0),
                        names.get(1));

                    String[] primaryKeys = getPrimaryNames(record);
                    String[] columns = getColumnNames(record);
                    if (useMerge) {
                        if (dbType == DbType.MYSQL) {
                            applierSql = SqlTemplates.MYSQL.getMergeSql(meta.getSchema(),
                                meta.getName(),
                                primaryKeys,
                                columns,
                                true);
                        } else if (dbType == DbType.DRDS) {
                            applierSql = SqlTemplates.MYSQL.getMergeSql(meta.getSchema(),
                                meta.getName(),
                                primaryKeys,
                                columns,
                                false);
                        } else if (dbType == DbType.ORACLE) {
                            applierSql = SqlTemplates.ORACLE.getMergeSql(meta.getSchema(),
                                meta.getName(),
                                primaryKeys,
                                columns);
                        }
                    } else {
                        if (dbType == DbType.MYSQL) {
                            // 如果mysql，全主键时使用insert ignore
                            applierSql = SqlTemplates.MYSQL.getInsertNomalSql(meta.getSchema(),
                                meta.getName(),
                                primaryKeys,
                                columns);
                        } else if (dbType == DbType.SUNDB) {
                            applierSql = SqlTemplates.SUNDB.getInsertSql(meta.getSchema(),
                                    meta.getName(),
                                    primaryKeys,
                                    columns);
                        } else {
                            applierSql = SqlTemplates.ORACLE.getInsertSql(meta.getSchema(),
                                meta.getName(),
                                primaryKeys,
                                columns);
                        }
                    }

                    int index = 1;
                    Map<String, Integer> indexs = new HashMap<String, Integer>();
                    for (String column : columns) {
                        indexs.put(column, index);
                        index++;
                    }

                    if (primaryKeys.length > 0 && !primaryKeys[0].equalsIgnoreCase("rowid")) {
                        for (String column : primaryKeys) {
                            indexs.put(column, index);
                            index++;
                        }
                    }

                    // 检查下是否少了列
                    checkColumns(meta, indexs);

                    sqlUnit.applierSql = applierSql;
                    sqlUnit.applierIndexs = indexs;
                    applierSqlCache.put(names, sqlUnit);
                }
            }
        }

        return sqlUnit;
    }
}
