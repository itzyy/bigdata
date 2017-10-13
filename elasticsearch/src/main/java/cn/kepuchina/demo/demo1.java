package cn.kepuchina.demo;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by Zouyy on 2017/8/17.
 *
 * 创建TransportClient这个接口，用来访问es集群
 *
 */
public class demo1  {

    public static void main(String[] args) {
        /**
         * 1.通过实现TranSportClient这个接口，我们可以不启动节点就可以与es集群进行通信，它需要指定，es集群中其中一台或者多台机器的ip地址和端口
         */
        TransportClient client = new TransportClient().addTransportAddress(
                new InetSocketTransportAddress("hadoop7",9300)
        ).addTransportAddress(
                new InetSocketTransportAddress("hadoop8",9300)
        );

        /**
         * 默认的es集群名称为：elasticsearch，如果发生更改需要如下设置
         */
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "myClusterName").build();
        TransportClient client1 = new TransportClient(settings).addTransportAddress(
                new InetSocketTransportAddress("hadoop7", 9300)
        );

        /**
         * 自动嗅探，es会自动将集群中其他机器的ip地址加到客户端中
         */

        Settings settings1 = ImmutableSettings.settingsBuilder().put("clinet.transport.sniff", true).build();
        TransportClient client2 = new TransportClient(settings1).addTransportAddress(
                new InetSocketTransportAddress("hadoop7", 9300)
        );

    }
}
