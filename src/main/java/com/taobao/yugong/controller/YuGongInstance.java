package com.taobao.yugong.controller;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.yugong.applier.FullRecordApplier;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.collect.Lists;
import com.taobao.yugong.applier.RecordApplier;
import com.taobao.yugong.common.YuGongConstants;
import com.taobao.yugong.common.alarm.AlarmMessage;
import com.taobao.yugong.common.alarm.AlarmService;
import com.taobao.yugong.common.audit.RecordDumper;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.RunMode;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.stats.ProgressTracer;
import com.taobao.yugong.common.stats.StatAggregation;
import com.taobao.yugong.common.stats.StatAggregation.AggregationItem;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.common.utils.thread.ExecutorTemplate;
import com.taobao.yugong.common.utils.thread.NamedThreadFactory;
import com.taobao.yugong.common.utils.thread.YuGongUncaughtExceptionHandler;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.RecordExtractor;
import com.taobao.yugong.positioner.RecordPositioner;
import com.taobao.yugong.translator.BackTableDataTranslator;
import com.taobao.yugong.translator.DataTranslator;
import com.taobao.yugong.translator.core.EncodeDataTranslator;
import com.taobao.yugong.translator.core.OracleIncreamentDataTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import javax.sql.DataSource;

/**
 * 代表一个同步迁移任务
 *
 * @author agapple 2013-9-17 下午3:21:01
 */
    public class YuGongInstance extends AbstractYuGongLifeCycle {

    private final Logger         logger          = LoggerFactory.getLogger(YuGongInstance.class);
    private YuGongContext        context;
    private RecordExtractor      extractor;             // 依赖 RunMode 决定使用什么类型的 Extractor.
    private RecordApplier        applier;
    private DataTranslator       translator;            // 如果有指定，使用指定的 TableName + DataTranslator，没有指定，使用 DefaultTranslator。
    private RecordPositioner     positioner;
    private AlarmService         alarmService;          // 全局唯一
    private String               alarmReceiver;         // 全局唯一
    private TableController      tableController;       // 全局唯一
    private ProgressTracer       progressTracer;        // 全局唯一
    private StatAggregation      statAggregation;
    private DbType               targetDbType;
    private boolean              tableExist;
    private AtomicBoolean inited = new AtomicBoolean(Boolean.FALSE);

    private List<DataTranslator> coreTranslators = Lists.newArrayList();
    private Thread               worker          = null;
    private volatile boolean     extractorDump   = true;
    private volatile boolean     applierDump     = true;
    private CountDownLatch       mutex           = new CountDownLatch(1);
    private YuGongException      exception       = null;
    private String               tableShitKey;
    private int                  retryTimes      = 1;
    private int                  retryInterval;
    private int                  noUpdateThresold;
    private int                  noUpdateTimes   = 0;

    // translator
    private boolean              concurrent      = true;
    private int                  threadSize      = 5;
    private ThreadPoolExecutor   executor;
    private String               executorName;

    public YuGongInstance(YuGongContext context){
        this.context = context;
        this.tableShitKey = context.getTableMeta().getFullName();
    }

    public void start() {
        MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
        super.start();

        try {
            tableController.acquire();// 尝试获取

            executorName = this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName();
            if (executor == null) {
                executor = new ThreadPoolExecutor(threadSize,
                    threadSize,
                    60,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue(threadSize * 2),
                    new NamedThreadFactory(executorName),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            }

            // 后续可改进为按类型识别添加
            coreTranslators.add(new OracleIncreamentDataTranslator());
            if (targetDbType.isOracle()) {
                coreTranslators.add(new EncodeDataTranslator(context.getSourceEncoding(), context.getTargetEncoding())); // oracle源库已经正确将编码转为'UTF-8'了
            }

            if (!positioner.isStart()) {
                positioner.start();
            }
            // xman-z: 设置上次读取的位置信息，这次接着读
            Position lastPosition = positioner.getLatest();
            context.setLastPosition(lastPosition);

            if (!extractor.isStart()) {
                extractor.start();
            }

            if (!applier.isStart()) {
                applier.start();
            }

            worker = new Thread(new Runnable() {
                public void run() {
                    try {
                        // 如果只是 mark 或 clear，只启动 extractor 就可以了。
                        if (context.getRunMode().isMark() || context.getRunMode().isClear()) {
                            return;
                        }

                        if (context.getRunMode() != RunMode.INC) {
                            // 目前只针对inc可以做重试
                            retryTimes = 1;
                        }

                        for (int i = 0; i < retryTimes; i++) {
                            MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
                            if (i > 0) {
                                logger.info("table[{}] is start , retrying ", context.getTableMeta().getFullName());
                            } else {
                                logger.info("table[{}] is start", context.getTableMeta().getFullName());
                            }

                            try {
                                // 处理几次重试，避免因短暂网络问题导致同步挂起
                                processTable();
                                exception = null;
                                break; // 处理成功就退出
                            } catch (YuGongException e) {
                                exception = e;
                                if (processException(e, i)) {
                                    break;
                                }
                            } finally {
                                MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
                            }
                        }

                        if (exception == null) {
                            // 记录到总文件下
                            logger.info("table[{}] is end", context.getTableMeta().getFullName());
                        } else if (ExceptionUtils.getRootCause(exception) instanceof InterruptedException) {
                            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
                            logger.info("table[{}] is interrpt ,current status:{} !", context.getTableMeta()
                                .getFullName(), extractor.status());
                        } else {
                            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
                            logger.info("table[{}] is error , current status:{} !", context.getTableMeta()
                                .getFullName(), extractor.status());
                        }
                    } finally {
                        tableController.release(YuGongInstance.this);
                        // 标记为成功
                        mutex.countDown();
                    }

                }

                private void processTable() {
                    try {
                        MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
                        ExtractStatus status = ExtractStatus.NORMAL;
                        AtomicLong batchId = new AtomicLong(0);
                        Position lastPosition = positioner.getLatest();
                        context.setLastPosition(lastPosition);
                        long tpsLimit = context.getTpsLimit();
                        do {
                            long start = System.currentTimeMillis();
                            // 提取数据
                            List<Record> records = extractor.extract();
                            List<Record> ackRecords = records;// 保留ack引用
                            if (YuGongUtils.isEmpty(records)) {
                                status = extractor.status();
                            }

                            // 判断是否记录日志
                            RecordDumper.dumpExtractorInfo(batchId.incrementAndGet(),
                                ackRecords,
                                lastPosition,
                                extractorDump);

                            // 是否有系统的translator处理
                            if (YuGongUtils.isNotEmpty(coreTranslators)) {
                                for (DataTranslator translator : coreTranslators) {
                                    records = processTranslator(translator, records);
                                }
                            }

                            // 转换数据
                            records = processTranslator(translator, records);

                            // 载入数据
                            Throwable applierException = null;
                            for (int i = 0; i < retryTimes; i++) {
                                try {
                                    createTableIfNeeded();
                                    if (!tableExist) {
                                        throw new YuGongException("table not exist!");
                                    }

                                    // onlyStruct == true: 只需要迁移表结构，数据不迁移。
                                    boolean onlyStruct = context.isOnlyStruct();
                                    if (!onlyStruct) {
                                        applier.apply(records);
                                    } else {
                                        status = ExtractStatus.TABLE_END;
                                        logger.info("record not need transfer.");
                                    }

                                    applierException = null;
                                    break;
                                } catch (Throwable e) {
                                    applierException = e;
                                    if (processException(e, i)) {
                                        break;
                                    }
                                }
                            }

                            if (applierException != null) {
                                throw applierException;
                            }

                            // 提供ack，进行后续处理
                            Position position = extractor.ack(ackRecords);
                            if (position != null) {
                                // 持久化位点信息，如果持久化失败，这批数据会重复
                                positioner.persist(position);
                            }

                            context.setLastPosition(position);
                            lastPosition = position;

                            // 判断是否记录日志
                            RecordDumper.dumpApplierInfo(batchId.get(), ackRecords, records, position, applierDump);

                            long end = System.currentTimeMillis();

                            if (tpsLimit > 0) {
                                tpsControl(ackRecords, start, end, tpsLimit);
                                end = System.currentTimeMillis();
                            }

                            if (YuGongUtils.isNotEmpty(ackRecords)) {
                                statAggregation.push(new AggregationItem(start, end, Long.valueOf(ackRecords.size())));
                            }

                            // 控制一下增量的退出
                            if (status == ExtractStatus.NO_UPDATE) {
                                noUpdateTimes++;
                                if (noUpdateThresold > 0 && noUpdateTimes > noUpdateThresold) {
                                    break;
                                }
                            }
                        } while (status != ExtractStatus.TABLE_END);

                        logger.info("table[{}] is end by {}", context.getTableMeta().getFullName(), status);
                        statAggregation.print();
                    } catch (InterruptedException e) {
                        // 正常退出，不发送报警
                        throw new YuGongException(e);
                    } catch (Throwable e) {
                        throw new YuGongException(e);
                    }
                }

                private void createTableIfNeeded() throws SQLException {
                    // 只在启动时执行一次。
                    if(inited.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
                        // 系统启动时，默认认为所有表都不存在
                        if (!tableExist) {
                            // 检查表是不是存在
                            tableExist = applier.isTableExist(context);
                        }

                        // 表存在，需要重建，先执行删除语句。
                        if (tableExist && context.isReCreateTableOnStart()) {
                            dropTable();
                            tableExist = applier.isTableExist(context);;
                        }

                        // 如果表不存在，创建。
                        if (!tableExist && context.isAutoCreateTable()) {
                            tableExist = applier.createTable(context);
                            // 表创建成功，再创建索引
                            if (tableExist && context.isAutoCreateIndex()) {
                                applier.createIndex(context);
                            }
                        }
                    }
                }

                private void dropTable() throws SQLException {
                    final String tableName = context.getTableMeta().getName();
                    final String sql = "drop table if exists " + context.getTableMeta().getName();
                    DataSource ds = context.getTargetDs();
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
                    jdbcTemplate.execute(sql, new PreparedStatementCallback() {
                        public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                            try {
                                ps.execute(sql);
                                logger.info("table [{}] drop success!", tableName);
                            } catch (SQLException e) {
                                logger.error("table [{}] drop failed! reason: {}", tableName, e.getMessage());
                                throw e;
                            }
                            return null;
                        }
                    });
                }

                private List<Record> processTranslator(final DataTranslator translator, List<Record> records) {
                    if (records.isEmpty()) {
                        return records;
                    }

                    if (translator != null) {
                        if (translator instanceof BackTableDataTranslator) {
                            ExecutorTemplate template = null;
                            if (concurrent) {
                                template = new ExecutorTemplate(executor);
                            }
                            records = ((BackTableDataTranslator) translator).translator(context.getSourceDs(),
                                context.getTargetDs(),
                                records,
                                template);
                        } else {
                            records = translator.translator(records);
                        }
                    }

                    return records;
                }

                private boolean processException(Throwable e, int i) {
                    if (!(ExceptionUtils.getRootCause(e) instanceof InterruptedException)) {
                        logger.error("retry {} ,something error happened. caused by {}",
                            (i + 1),
                            ExceptionUtils.getFullStackTrace(e));
                        try {
                            alarmService.sendAlarm(new AlarmMessage(ExceptionUtils.getFullStackTrace(e), alarmReceiver));
                        } catch (Throwable e1) {
                            logger.error("send alarm failed. ", e1);
                        }

                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException e1) {
                            exception = new YuGongException(e1);
                            Thread.currentThread().interrupt();
                            return true;
                        }
                    } else {
                        // interrupt事件，响应退出
                        return true;
                    }

                    return false;
                }

            });

            worker.setUncaughtExceptionHandler(new YuGongUncaughtExceptionHandler(logger));
            worker.setName(this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName());
            worker.start();

            logger.info("table[{}] start successful. extractor:{} , applier:{}, translator:{}", new Object[] {
                    context.getTableMeta().getFullName(), extractor.getClass().getName(), applier.getClass().getName(),
                    translator != null ? translator.getClass().getName() : "NULL" });
        } catch (InterruptedException e) {
            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
            exception = new YuGongException(e);
            mutex.countDown();
            tableController.release(this); // 释放下
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
            exception = new YuGongException(e);
            mutex.countDown();
            logger.error("table[{}] start failed caused by {}",
                context.getTableMeta().getFullName(),
                ExceptionUtils.getFullStackTrace(e));
            tableController.release(this); // 释放下
        }
    }

    /**
     * 等待instance处理完成
     *
     * @throws InterruptedException
     */
    public void waitForDone() throws InterruptedException, YuGongException {
        mutex.await();

        if (exception != null) {
            throw exception;
        }
    }

    public void stop() {
        MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
        super.stop();

        // 尝试中断
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(2 * 1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        if (extractor.isStart()) {
            extractor.stop();
        }

        if (applier.isStart()) {
            applier.stop();
        }

        if (positioner.isStart()) {
            positioner.stop();
        }

        executor.shutdownNow();

        exception = null;
        logger.info("table[{}] stop successful. ", context.getTableMeta().getFullName());
    }

    private void tpsControl(List<Record> result, long start, long end, long tps) throws InterruptedException {
        long expectTime = (result.size() * 1000) / tps;
        long runTime = expectTime - (end - start);
        if (runTime > 0) {
            Thread.sleep(runTime);
        }
    }

    public RecordExtractor getExtractor() {
        return extractor;
    }

    public void setExtractor(RecordExtractor extractor) {
        this.extractor = extractor;
    }

    public RecordApplier getApplier() {
        return applier;
    }

    public void setApplier(RecordApplier applier) {
        this.applier = applier;
    }

    public DataTranslator getTranslator() {
        return translator;
    }

    public void setTranslator(DataTranslator translator) {
        this.translator = translator;
    }

    public RecordPositioner getPositioner() {
        return positioner;
    }

    public void setPositioner(RecordPositioner positioner) {
        this.positioner = positioner;
    }

    public void setAlarmService(AlarmService alarmService) {
        this.alarmService = alarmService;
    }

    public void setExtractorDump(boolean extractorDump) {
        this.extractorDump = extractorDump;
    }

    public void setApplierDump(boolean applierDump) {
        this.applierDump = applierDump;
    }

    public void setTableController(TableController tableController) {
        this.tableController = tableController;
    }

    public void setTableShitKey(String tableShitKey) {
        this.tableShitKey = tableShitKey;
    }

    public void setStatAggregation(StatAggregation statAggregation) {
        this.statAggregation = statAggregation;
    }

    public void setAlarmReceiver(String alarmReceiver) {
        this.alarmReceiver = alarmReceiver;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public void setTargetDbType(DbType targetDbType) {
        this.targetDbType = targetDbType;
    }

    public YuGongContext getContext() {
        return context;
    }

    public void setProgressTracer(ProgressTracer progressTracer) {
        this.progressTracer = progressTracer;
    }

    public void setNoUpdateThresold(int noUpdateThresold) {
        this.noUpdateThresold = noUpdateThresold;
    }

    public void setThreadSize(int threadSize) {
        this.threadSize = threadSize;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

}
