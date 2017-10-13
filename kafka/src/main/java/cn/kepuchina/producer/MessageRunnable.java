package cn.kepuchina.producer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Created by LENOVO on 2017/8/4.
 */
public class MessageRunnable implements Runnable {
    private KafkaStream<byte[], byte[]> consumer;

    public MessageRunnable(KafkaStream<byte[], byte[]> consumer) {
        this.consumer= consumer;
    }
    public void run(){
        for (MessageAndMetadata<byte[], byte[]> item : consumer) {
            System.out.println(String.format("thread:%s   partition:%s    offset:%s   message:%s", Thread.currentThread().getName(), item.partition(), item.offset(), new String(item.message())));
        }
    }


//    private KafkaStream<byte[],byte[]> kafkaStream;
//
//    public MessageRunnable(KafkaStream<byte[],byte[]> kafkaStream) {
//        this.kafkaStream = kafkaStream;
//    }
//
//    public void run() {
//        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
//        while (iterator.hasNext()) {
//            MessageAndMetadata<byte[], byte[]> item = iterator.next();
//            System.out.println(String.format("thread:%s   partition:%s    offset:%s   message:%s",Thread.currentThread().getName(),item.partition(),item.offset(),String.valueOf(item.message())));
//        }
//    }
}
