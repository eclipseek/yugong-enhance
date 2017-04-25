package com.taobao.yugong.common.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.RunMode;

/**
 * ͳ���µ�ǰ����Ǩ�Ƶ�״̬
 * 
 * @author agapple 2014-4-24 ����2:12:13
 * @since 3.0.4
 */
public class ProgressTracer {

    private static final Logger         logger       = LoggerFactory.getLogger(ProgressTracer.class);
    private static final String         FULL_FORMAT  = "{δ����:%s,ȫ����:%s,�����:%s,�쳣��:%s}";
    private static final String         INC_FORMAT   = "{δ����:%s,������:%s,��׷��:%s,�쳣��:%s}";
    private static final String         CHECK_FORMAT = "{δ����:%s,�Ա���:%s,�����:%s,�쳣��:%s}";
    private static final String         ALL_FORMAT   = "{δ����:%s,ȫ����:%s,������:%s,��׷��:%s,�쳣��:%s}";

    private int                         total;
    private RunMode                     mode;
    private Map<String, ProgressStatus> status       = new ConcurrentHashMap<String, ProgressStatus>();

    public ProgressTracer(RunMode mode, int total){
        this.mode = mode;
        this.total = total;
    }

    public void update(String tableName, ProgressStatus progress) {
        ProgressStatus st = status.get(tableName);
        if (st != ProgressStatus.FAILED) {
            status.put(tableName, progress);
        }
    }

    public void printSummry() {
        print(false);
    }

    public void print(boolean detail) {
        int fulling = 0;
        int incing = 0;
        int failed = 0;
        int success = 0;
        List<String> fullingTables = new ArrayList<String>();
        List<String> incingTables = new ArrayList<String>();
        List<String> failedTables = new ArrayList<String>();
        List<String> successTables = new ArrayList<String>();

        for (Map.Entry<String, ProgressStatus> entry : status.entrySet()) {
            ProgressStatus progress = entry.getValue();
            if (progress == ProgressStatus.FULLING) {
                fulling++;
                fullingTables.add(entry.getKey());
            } else if (progress == ProgressStatus.INCING) {
                incing++;
                incingTables.add(entry.getKey());
            } else if (progress == ProgressStatus.FAILED) {
                failed++;
                failedTables.add(entry.getKey());
            } else if (progress == ProgressStatus.SUCCESS) {
                success++;
                successTables.add(entry.getKey());
            }
        }

        int unknow = this.total - fulling - incing - failed - success;
        String msg = null;
        if (mode == RunMode.ALL) {
            msg = String.format(ALL_FORMAT, new Object[] { unknow, fulling, incing, success, failed });
        } else if (mode == RunMode.FULL) {
            msg = String.format(FULL_FORMAT, new Object[] { unknow, fulling, success, failed });
        } else if (mode == RunMode.INC) {
            msg = String.format(INC_FORMAT, new Object[] { unknow, incing, success, failed });
        } else if (mode == RunMode.CHECK) {
            msg = String.format(CHECK_FORMAT, new Object[] { unknow, fulling, success, failed });
        }

        logger.info("{}", msg);
        if (detail) {
            if (fulling > 0) {
                if (mode == RunMode.CHECK) {
                    logger.info("�Ա���:" + fullingTables);
                } else {
                    logger.info("ȫ����:" + fullingTables);
                }
            }
            if (incing > 0) {
                logger.info("������:" + incingTables);
            }
            if (failed > 0) {
                logger.info("�쳣��:" + failedTables);
            }
            logger.info("�����:" + successTables);
        }
    }

    public static void main(String args[]) {
        System.out.println("���Ĳ���/");
    }
}
