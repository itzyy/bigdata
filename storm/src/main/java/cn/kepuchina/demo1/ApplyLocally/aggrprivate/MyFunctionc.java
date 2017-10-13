package cn.kepuchina.demo1.ApplyLocally.aggrprivate;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class MyFunctionc extends BaseFunction {
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        for (int i = 0; i < tridentTuple.getInteger(0); i++) {
            tridentCollector.emit(new Values(i));
        }
    }
}
