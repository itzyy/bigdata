package cn.kepuchina.demo1.ApplyLocally.spout;

import clojure.lang.Obj;
import cn.kepuchina.trident.filter.DiseaseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Zouyy on 2017/8/9.
 * Emitter函数只有一个功能，将tuple打包发射出去，
 */
public class MyEmitter implements ITridentSpout.Emitter<Long>,Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MyEmitter.class);
    private AtomicInteger successfulTransaction = new AtomicInteger();

    /**
     * @param transactionAttempt 由coordinator生成的元数据
     * @param aLong 事务id
     * @param tridentCollector 用来发送tuple的collector
     */
    public void emitBatch(TransactionAttempt transactionAttempt, Long aLong, TridentCollector tridentCollector) {
        ArrayList<Object> list =new ArrayList<Object>();
        list.add(1);
        list.add(2);
        list.add(3);
        tridentCollector.emit(list);

    }

    public void success(TransactionAttempt transactionAttempt) {
        successfulTransaction.incrementAndGet();
    }

    public void close() {

    }
}
