package cn.kepuchina.consumer;

import cn.kepuchina.producer.MessageRunnable;
import cn.kepuchina.producer.ProducerDemo;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by LENOVO on 2017/8/3.
 */
public class ConsumerDemo {

    public static void main(String[] args) {
        Properties prop = new Properties();

        //获取配置文件对象
        try {
            prop.load(ProducerDemo.class.getClassLoader().getResourceAsStream("consumer.properties"));
            System.out.println(prop.getProperty("metadata.broker.list"));
            //创建consumer链接
            ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
            //创建consumer topic访问连接数
            HashMap<String, Integer> topicCountMap = new HashMap<String, Integer>();
            Integer consumerNum = 3;
            String  topic = "kepuchina2";
            //配置访问topic数量
            topicCountMap.put(topic,consumerNum);
            //获取topic的读取流
            Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = connector.createMessageStreams(topicCountMap);
            KafkaStream<byte[], byte[]> consumers = topicMessageStreams.get(topic).get(0);
//            Thread t = new Thread(new MessageRunnable(consumers));
//            t.start();
//            Thread t1 = new Thread(new MessageRunnable(consumers));
//            t1.start();
            ExecutorService threadPool= Executors.newFixedThreadPool(consumerNum);
            threadPool.execute(new MessageRunnable(consumers));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //输出相应的文件内容
    }
}
