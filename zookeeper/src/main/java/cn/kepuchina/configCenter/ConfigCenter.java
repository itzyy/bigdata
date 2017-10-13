package cn.kepuchina.configCenter;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by Zouyy on 2017/9/4.
 */
public class ConfigCenter implements Watcher{


    private ZooKeeper zk ;
    private String znode;
    private String address;

    public ConfigCenter(String address,String znode) {
        this.znode = znode;
        try {
            this.zk = new ZooKeeper(address,300,this);
            Stat stat = this.zk.exists(znode,true);
            if(stat==null){
                this.zk.create(znode,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void updateConfig(String str){
        try {
            Stat stat = this.zk.exists(this.znode, true);
            this.zk.setData(this.znode,str.getBytes(), stat.getVersion());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 监控
     */
    public void process(WatchedEvent event) {
        System.out.println(event.toString());
        try {
            this.zk.exists("/root1",true);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
