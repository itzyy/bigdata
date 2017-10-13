package cn.kepuchina.configCenter;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by Zouyy on 2017/9/4.
 */
public class ZookeeperWatcher implements Watcher, Runnable {


    private ZooKeeper zk;

    private String znode;

    public void connect(String hosts, String znode) {

        try {
            this.zk = new ZooKeeper(hosts, 2000, this);
            this.znode = znode;
            this.zk.exists(znode, true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    public void setData(byte[] data) {
        try {
            Stat stat = this.zk.exists(znode, false);
            this.zk.setData(znode,data,stat.getVersion());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void run() {
        synchronized (this) {
            while (true) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void process(WatchedEvent event) {
        System.out.println(event.toString());
        try {
            this.zk.exists(znode, true);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
