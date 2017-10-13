package cn.kepuchina.trident.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;

/**
 * Created by Zouyy on 2017/8/9.
 * 提取出特定城市、疾病、时间的发生次数，并且检查计数是否超过了设定阈值，如果超过了，发送一个新的字段包括一条告警信息
 */
public class OutbreakDetector extends BaseFunction {

    private static final long serialVersionUID =1l;

    public static final int THRESHOLD =10000;

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String key = (String) tridentTuple.getValue(0);
        Long  count = (Long) tridentTuple.getValue(1);

        if(count > THRESHOLD){
            ArrayList<Object> values = new ArrayList<Object>();
            values.add("Outbreak detected for ["+key+"]!");
            tridentCollector.emit(values);
        }
    }
}
