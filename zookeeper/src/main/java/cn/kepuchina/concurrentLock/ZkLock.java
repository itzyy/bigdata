package cn.kepuchina.concurrentLock;

/**
 * Created by Zouyy on 2017/8/21.
 */
public class ZkLock implements Runnable {

    private DistributedLock lock;
    private String zkConnString;
    private String lockName;

    public ZkLock(String zkConnString, String lockName) {
        this.zkConnString = zkConnString;
        this.lockName = lockName;
    }

    public void run() {
        try {
            lock = new DistributedLock(zkConnString, lockName);
            lock.lock();
            Thread.sleep(1000);
            System.out.println("Thread:"+Thread.currentThread().getId()+" running");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            if (lock!=null) {
                lock.unlock();
            }
        }
    }
}
