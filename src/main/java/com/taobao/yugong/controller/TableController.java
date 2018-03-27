package com.taobao.yugong.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * 简单利用信号量控制table并发数
 * 
 * @author agapple 2013-9-23 下午10:48:34
 */
public class TableController {

    private CountDownLatch                      countLatch;
    private Semaphore                           sem;
    private LinkedBlockingQueue<YuGongInstance> queue = new LinkedBlockingQueue<YuGongInstance>();

    public TableController(int total, int cocurrent){
        this.countLatch = new CountDownLatch(total);

        // comment by zhangyq: concurrent 是所有表数据同步共用的线程数。
        this.sem = new Semaphore(cocurrent);
    }

    public void acquire() throws InterruptedException {
        sem.acquire();
    }

    public void release(YuGongInstance instance) {
        sem.release();
        queue.offer(instance);
        countLatch.countDown();
    }

    // 获取已经结束的（包括正常结束和异常结束的）
    public YuGongInstance takeDone() throws InterruptedException {
        return queue.take();
    }

    public void waitForDone() throws InterruptedException {
        countLatch.await();
    }

}
