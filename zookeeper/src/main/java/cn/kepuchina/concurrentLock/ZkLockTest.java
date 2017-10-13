package cn.kepuchina.concurrentLock;

/**
 * Created by Zouyy on 2017/8/22.
 */
public class ZkLockTest {

    private static final String zkConnString = "hadoop7:2181,hadoop8:2181,hadoop9:2181";

    public static void main(String[] args) {

//        ExecutorService pool = Executors.newFixedThreadPool(9);
//        pool.execute(new ZkLock(zkConnString, "test1"));
//        pool.execute(new ZkLock(zkConnString, "test2"));
//        pool.execute(new ZkLock(zkConnString, "test3"));
//        pool.execute(new ZkLock(zkConnString, "test4"));
//        pool.execute(new ZkLock(zkConnString, "test5"));
//        pool.execute(new ZkLock(zkConnString, "test6"));
//        pool.execute(new ZkLock(zkConnString, "test7"));
//        pool.execute(new ZkLock(zkConnString, "test8"));
//        pool.execute(new ZkLock(zkConnString, "test9"));

//        new Thread(new ZkLock(zkConnString, "Test")).start();
        ConcurrentTask[] tasks = new ConcurrentTask[60];
        for (int i = 0; i < tasks.length; i++) {
            ConcurrentTask task = new ConcurrentTask() {
                public void run() {
                    new Thread(new ZkLock(zkConnString, "test1")).start();
                }
            };
            tasks[i]=task;
        }
        new ConcurrentTest(tasks);
    }


}

