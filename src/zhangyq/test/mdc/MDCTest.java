package mdc;

import org.slf4j.MDC;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2016-12-27
 * Time: 21:33
 */
public class MDCTest  extends Thread {
    private int i ;

    public MDCTest(int i){
        this.i = i;
    }

    public void run(){
        // 相同的 key，但不同的线程不会覆盖其他线程添加 value
        MDC.put("username", i + "");
        System.out.println(Thread.currentThread().getName() + " ---> " + MDC.get("username"));
    }

    public static void main(String args[]) throws InterruptedException {
        MDCTest t1 = new MDCTest(1);
        t1.start();
        MDCTest t2 = new MDCTest(2);
        t2.start();

        t1.join();
        t2.join();
    }
}
