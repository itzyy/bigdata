package cn.kepuchina.trident.spout;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Zouyy on 2017/8/9.
 * Emitter函数只有一个功能，将tuple打包发射出去，
 */
public class DiagnosisEventEmitter implements ITridentSpout.Emitter<Long>,Serializable {

    private AtomicInteger successfulTransaction = new AtomicInteger();
    /**
     * @param transactionAttempt 由coordinator生成的元数据
     * @param aLong 事务id
     * @param tridentCollector 用来发送tuple的collector
     */
    public void emitBatch(TransactionAttempt transactionAttempt, Long aLong, TridentCollector tridentCollector) {
        for (int i = 0; i < 10000; i++) {
            ArrayList<Object> events = new ArrayList<Object>();
            double lat = new Double(-30+(int)(Math.random()*75));
            double lng = new Double(-120 + (int) (Math.random() * 70));
            String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
            //生成诊断的时间戳
            long time = System.currentTimeMillis();
            //
            DiagnosisEvent event = new DiagnosisEvent(lat,lng,time,diag);
            events.add(event);
            tridentCollector.emit(events);
        }
    }

    public void success(TransactionAttempt transactionAttempt) {
        successfulTransaction.incrementAndGet();
    }

    public void close() {

    }
}
