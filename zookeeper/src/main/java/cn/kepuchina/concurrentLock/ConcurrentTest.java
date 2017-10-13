package cn.kepuchina.concurrentLock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Zouyy on 2017/8/21.
 */
public class ConcurrentTest {

    //任务集合
    private ConcurrentTask[] tasks = null;
    //开始阀门
    private CountDownLatch startSianal = new CountDownLatch(1);
    //结束阀门
    private CountDownLatch doneSignal = null;

    private CopyOnWriteArrayList list = new CopyOnWriteArrayList();
    //原子递增
    private AtomicInteger err = new AtomicInteger();


    public ConcurrentTest(ConcurrentTask... tasks) {


        this.tasks = tasks;
        if (tasks == null) {
            System.out.println("task cat not null");
            //非正常退出！
            System.exit(1);
        }
        //设置计数器的数量
        doneSignal = new CountDownLatch(tasks.length);
        //开始创建线程
        start();

    }


    private void start() {
        //开始创建线程
        createThread();
        //计数器减一，如果计数到达零，则释放所有等待的线程
        startSianal.countDown();
        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //计算执行时间
        getExeTime();
    }

    /**
     * 初始化所有线程
     */
    public void createThread() {
        long len = doneSignal.getCount();
        for (int i = 0; i < len; i++) {
            final int j = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        //等待当前线程在锁存器倒计数至零之前一直等待
                        startSianal.await();
                        long start = System.currentTimeMillis();
                        tasks[j].run();
                        long end = System.currentTimeMillis() - start;
                        list.add(end);
                    } catch (Exception e) {
                        //相当于err++
                        err.getAndIncrement();
                    }
                    //任务执行完减一
                    doneSignal.countDown();
                }
            }).start();//启动线程
        }
    }

    /**
     * 计算执行时间
     */
    public void getExeTime(){
        int size = list.size();
        ArrayList<Long> _list = new ArrayList<Long>();
        _list.addAll(list);
        Collections.sort(_list);
        Long min = _list.get(0);
        Long max = _list.get(size - 1);
        long sum = 0L;
        for (Long t :
                _list) {
            sum+=t;
        }
        long avg = sum/size;
        System.out.println("min:"+min);
        System.out.println("max:"+max);
        System.out.println("avg:"+avg);
        System.out.println("err:"+err);
    }
}
