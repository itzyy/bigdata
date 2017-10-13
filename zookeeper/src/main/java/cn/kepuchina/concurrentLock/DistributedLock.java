package cn.kepuchina.concurrentLock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Zoopkeeper分布式锁
 * 获取锁实现思路
 * 1、首先创建一个作为锁目录（znode）,通常用它来描述锁定的实体，称为:/locak_node
 * 2、希望获取锁的客户端在锁目录下创建znode,做为锁/lock_node的子节点，并且节点类型为有序临时节点（ephemeral_sequential）
 * 例如：有两个客户端创建znode,分别为/lock_node/lock-1和/lock_node/lock-2
 * 3、当前客户端调用getChildren(/lock_node)得到锁目录所有子节点，不设置watch，接着获取小于自己(步骤2创建)的兄弟节点
 * 4、步骤3中获取小于自己的节点不存在&&最小节点与步骤2中创建的相同，说明当前客户端顺序号最小，获得锁，结束。
 * 5、客户端监视（watch）相对自己次小的有序临时节点状态
 * 6、如果监视的次小节点状态发生变化，则跳转到步骤3.继续后续操作，知道退出锁竞争
 */
public class DistributedLock implements Lock, Watcher {

    //zk对象
    private ZooKeeper zk;
    //session超时：30s
    private int sessionTimeOut = 30000;
    //锁目录
    private String root = "/locks";
    //竞争资源的标志
    private String lockName;
    //当前锁
    private String myZnode;
    //等待前一个锁
    private String waitNode;
    //异常集合
    List<Exception> exceptions = new ArrayList<Exception>();
    //等待时间
    private long waitTimeOut = 30000;
    //计数器
    private CountDownLatch latch;

    public DistributedLock(String zkConnString, String lockName) {
        this.lockName = lockName;
        //创建zk服务
        try {
            zk = new ZooKeeper(zkConnString, sessionTimeOut, this);
            Stat stat = zk.exists(root, false);
            //返回状态为null则创建父节点
            if (stat == null) {
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException e) {
            exceptions.add(e);
        } catch (KeeperException e) {
            exceptions.add(e);
        } catch (IOException e) {
            exceptions.add(e);
        }

    }

    /**
     * 已经获取锁了
     */
    public void lock() {
        if (exceptions.size() < 0) {
            throw new LockException(exceptions.get(0));
        }

        //开始尝试获取锁
        if (this.tryLock()) {
            System.out.println("=====已经获取锁了--->Tread：" + Thread.currentThread().getId() + " " + myZnode);
        } else {
            //等待获取锁
            this.waitForLock(waitNode, waitTimeOut);
        }

    }

    /**
     * 中断操作
     *
     * @throws InterruptedException
     */
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    /**
     * 开始尝试获取锁
     *
     * @return
     */
    public boolean tryLock() {
        String splitStr = "_lock_";
        if (lockName.contains(splitStr))
            throw new LockException("lockname 不能包含contains \\u000B");
        try {
            myZnode = zk.create(root + "/" + this.lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            if(myZnode==null)
                throw new LockException("myZnode为空！");

            System.err.println( "+++++开始创建--->Thread:"+Thread.currentThread().getId()+"  myZnode:"+myZnode);
            //取出所有的子节点
            List<String> childrens = zk.getChildren(root, false);
            //获得所有的锁
            ArrayList<String> lockObjNods = new ArrayList<String>();
            for (String node :
                    childrens) {
                String _node = node.split(splitStr)[0];
                if (_node.equals(lockName)) {
                    lockObjNods.add(node);
                }
            }
            //对节点进行默认排序，从小到大
            Collections.sort(lockObjNods);
           System.out.println("确认当前myZnode是否是最小路径："+myZnode + "==" + lockObjNods.get(0));
            if (myZnode.equals(root + "/" + lockObjNods.get(0))) {
                //如果是最小节点，则表示取得锁
                return true;
            }
            //如果不是最小节点，则找到比自己小1的节点
            String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
            System.out.println("《》《》《》《》找到比自己小的节点："+subMyZnode);
            waitNode = lockObjNods.get(Collections.binarySearch(lockObjNods, subMyZnode) - 1);

        } catch (KeeperException e) {
            e.printStackTrace();
//            throw new LockException(e);
        } catch (InterruptedException e) {
//            throw new LockException(e);
        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) {
        if (this.tryLock()) {
            return true;
        } else {
            return waitForLock(waitNode, time);
        }
    }


    /**
     * 等待获取锁
     *
     * @param waitZnode
     * @param waitTimeOut
     * @return
     */
    private boolean waitForLock(String waitZnode, long waitTimeOut) {
        try {
            Stat stat = zk.exists(root + "/" + waitZnode, true);
            if (stat != null) {
                System.out.println("》》》》》等待线程：Thread:" + Thread.currentThread().getId() + "  waiting for " + root + "/" + waitZnode);
                this.latch = new CountDownLatch(1);
                //等待现场执行完毕：等待时长waitTimeOut
                this.latch.await(waitTimeOut, TimeUnit.MILLISECONDS);
                this.latch = null;
            }

        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
        return true;
    }

    /**
     * 取消锁监控
     */
    public void unlock() {
        try {
            System.out.println("***** 取消锁监控--->Thread:"+Thread.currentThread().getId() + ",unlock:" + myZnode);
            zk.delete(myZnode, -1);
            myZnode = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public Condition newCondition() {
        return null;
    }


    /**
     * zookeeper的监视器
     *
     * @param event
     */
    public void process(WatchedEvent event) {

        if (this.latch == null) {
            //计数器减一
            this.latch.countDown();
        }

    }

    /**
     * 关闭zk
     */
    public void closeZk() {
        try {
            zk.close();
        } catch (Exception e) {

        }
    }

    /**
     * 自定义异常消息
     *
     * @author zouyy
     */
    public class LockException extends RuntimeException {
        public static final long serialVersionUID = 1L;

        public LockException(String message) {
            super(message);
        }

        public LockException(Exception e) {
            super(e);
        }
    }

}
