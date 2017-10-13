package cn.kepuchina.configCenter;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Zouyy on 2017/9/4.
 */
public class Test {

    public static void main(String[] args) {



        String host ="hadoop7:2181,hadoop8:2181,hadoop9:2181";
        String znode="/root2";
        ZookeeperWatcher zw1 = new ZookeeperWatcher();
        zw1.connect(host,znode);

        ZookeeperWatcher zw2 = new ZookeeperWatcher();
        zw2.connect(host,znode);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.execute(zw1);
        pool.execute(zw2);

        ConfigCenter cc = new ConfigCenter(host, znode);
        cc.updateConfig("a");
        cc.updateConfig("b");
        cc.updateConfig("c");
        cc.updateConfig("d");
        cc.updateConfig("e");


    }
}
