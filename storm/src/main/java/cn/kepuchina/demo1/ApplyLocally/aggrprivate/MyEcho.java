package cn.kepuchina.demo1.ApplyLocally.aggrprivate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class MyEcho extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(MyEcho.class);

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        for (Object obj : tridentTuple) {

            LOG.info("-----------["+obj+"]-----------");
        }
    }
}
