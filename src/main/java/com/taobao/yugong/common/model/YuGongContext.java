package com.taobao.yugong.common.model;

import javax.sql.DataSource;

import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.model.position.Position;

import java.util.ArrayList;
import java.util.List;

/**
 * yugong数据处理上下文
 * 
 * @author agapple 2013-9-12 下午5:04:57
 */
public class YuGongContext {

    // 具体每张表的同步
    private Position   lastPosition;                  // 最后一次同步的position记录
    private Table      tableMeta;                     // 对应的meta
    private boolean    ignoreSchema         = false;  // 同步时是否忽略schema，oracle迁移到mysql可能schema不同，可设置为忽略

    // 全局共享
    private int        buildThreadNum;
    private boolean    autoCreateTable      = false;    // 同步数据时，如果表不存在，是否自动创建表
    private boolean    autoCreateIndex      = false;    // 是否自动创建索引。
    private boolean    onlyStruct           = false;    // 是否只创建表结构，不迁移数据
    // stinson
    private boolean addPrimaryKey = false;              // 如果不存在主键，是否将 oracle 中 rowid 作为主键。
    private boolean reCreateTableOnStart = false;              // 启动时重建表
    private boolean applierWithTransaction = false;
    private RunMode    runMode;
    private int        onceCrawNum;                   // 每次提取的记录数
    private int        tpsLimit             = 0;      // <=0代表不限制
    private DataSource sourceDs;                      // 源数据库链接
    private DataSource targetDs;                      // 目标数据库链接

    private boolean    batchApply           = false;
    private boolean    skipApplierException = false;  // 是否允许跳过applier异常
    private String     sourceEncoding       = "UTF-8";
    private String     targetEncoding       = "UTF-8";

    public Position getLastPosition() {
        return lastPosition;
    }

    public void setLastPosition(Position lastPosition) {
        this.lastPosition = lastPosition;
    }

    public int getOnceCrawNum() {
        return onceCrawNum;
    }

    public void setOnceCrawNum(int onceCrawNum) {
        this.onceCrawNum = onceCrawNum;
    }

    public DataSource getSourceDs() {
        return sourceDs;
    }

    public void setSourceDs(DataSource sourceDs) {
        this.sourceDs = sourceDs;
    }

    public DataSource getTargetDs() {
        return targetDs;
    }

    public void setTargetDs(DataSource targetDs) {
        this.targetDs = targetDs;
    }

    public boolean isBatchApply() {
        return batchApply;
    }

    public void setBatchApply(boolean batchApply) {
        this.batchApply = batchApply;
    }

    public String getSourceEncoding() {
        return sourceEncoding;
    }

    public void setSourceEncoding(String sourceEncoding) {
        this.sourceEncoding = sourceEncoding;
    }

    public String getTargetEncoding() {
        return targetEncoding;
    }

    public void setTargetEncoding(String targetEncoding) {
        this.targetEncoding = targetEncoding;
    }

    public Table getTableMeta() {
        return tableMeta;
    }

    public void setTableMeta(Table tableMeta) {
        this.tableMeta = tableMeta;
    }

    public int getTpsLimit() {
        return tpsLimit;
    }

    public void setTpsLimit(int tpsLimit) {
        this.tpsLimit = tpsLimit;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    public boolean isIgnoreSchema() {
        return ignoreSchema;
    }

    public void setIgnoreSchema(boolean ignoreSchema) {
        this.ignoreSchema = ignoreSchema;
    }

    public boolean isSkipApplierException() {
        return skipApplierException;
    }

    public void setSkipApplierException(boolean skipApplierException) {
        this.skipApplierException = skipApplierException;
    }

    public int getBuildThreadNum() {
        return buildThreadNum;
    }

    public YuGongContext setBuildThreadNum(int buildThreadNum) {
        this.buildThreadNum = buildThreadNum;
        return this;
    }

    public boolean isAutoCreateTable() {
        return autoCreateTable;
    }

    public YuGongContext setAutoCreateTable(boolean autoCreateTable) {
        this.autoCreateTable = autoCreateTable;
        return this;
    }

    public boolean isAutoCreateIndex() {
        return autoCreateIndex;
    }

    public YuGongContext setAutoCreateIndex(boolean autoCreateIndex) {
        this.autoCreateIndex = autoCreateIndex;
        return this;
    }

    public boolean isOnlyStruct() {
        return onlyStruct;
    }

    public YuGongContext setOnlyStruct(boolean onlyStruct) {
        this.onlyStruct = onlyStruct;
        return this;
    }

    public boolean isAddPrimaryKey() {
        return addPrimaryKey;
    }

    public void setAddPrimaryKey(boolean addPrimaryKey) {
        this.addPrimaryKey = addPrimaryKey;
    }

    public boolean isReCreateTableOnStart() {
        return reCreateTableOnStart;
    }

    public void setReCreateTableOnStart(boolean reCreateTableOnStart) {
        this.reCreateTableOnStart = reCreateTableOnStart;
    }

    public boolean isApplierWithTransaction() {
        return applierWithTransaction;
    }

    public void setApplierWithTransaction(boolean applierWithTransaction) {
        this.applierWithTransaction = applierWithTransaction;
    }

    public YuGongContext cloneGlobalContext() {
        YuGongContext context = new YuGongContext();
        context.setRunMode(runMode);
        context.setBatchApply(batchApply);
        context.setSourceDs(sourceDs);
        context.setTargetDs(targetDs);
        context.setSourceEncoding(sourceEncoding);
        context.setTargetEncoding(targetEncoding);
        context.setBuildThreadNum(buildThreadNum);
        context.setAutoCreateTable(autoCreateTable);
        context.setOnlyStruct(onlyStruct);
        context.setAutoCreateIndex(autoCreateIndex);
        context.setAddPrimaryKey(addPrimaryKey);
        context.setReCreateTableOnStart(reCreateTableOnStart);
        context.setApplierWithTransaction(applierWithTransaction);
        context.setOnceCrawNum(onceCrawNum);
        context.setTpsLimit(tpsLimit);
        context.setIgnoreSchema(ignoreSchema);
        context.setSkipApplierException(skipApplierException);
        return context;
    }
}
