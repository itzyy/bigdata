package cn.kepuchina.trident.function;

import cn.kepuchina.trident.filter.DiseaseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Zouyy on 2017/8/9.
 */
public class DispatchAlert extends BaseFunction {

    private static final long serialVersionUID=1l;
    private static final Logger LOG =  LoggerFactory.getLogger(DiseaseFilter.class);

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String alert = (String)tridentTuple.getValue(0);
        LOG.error("警报信息 【"+alert+"】");
        LOG.error("发生疫情了，快来人。。。。。。人。。。。。人。。。。。。。人。。。。。。。。。。。。人。。。。。。。。");
        System.exit(0);
    }
}