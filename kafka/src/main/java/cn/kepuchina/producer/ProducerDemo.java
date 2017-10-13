package cn.kepuchina.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by LENOVO on 2017/8/3.
 */
public class ProducerDemo {

    public static void main(String[] args) {

        Properties prop = new Properties();
        try {
            //获取配置文件对象
            prop.load(ProducerDemo.class.getClassLoader().getResourceAsStream("producer.properties"));
            //输出相应的文件内容
            System.out.println(prop.getProperty("metadata.broker.list"));
            //获取producer对象
            Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(prop));

            /*
             props.put("serializer.class", "kafka.serializer.StringEncoder");
             props.put("metadata.broker.list", "192.168.1.164:9093"); // 配置kafka端口
             props.put("partitioner.class","com.kafka.myparitioner.CidPartitioner");*/


             producer.send(new KeyedMessage<String,String>("hello","world"));

           //创建topic


//            producer.send(new KeyedMessage<String, String>("kepuchina2", "1"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "2"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "3"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "4"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "5"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "6"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "7"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "8"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "9"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "10"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "11"));
//            producer.send(new KeyedMessage<String, String>("kepuchina2", "12"));
            producer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
