package cn.kepuchina.trident.Aggregator;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Zouyy on 2017/8/10.
 * 使用分组和计数来统计在一个城市附近一个小时内发生疾病的次数，实现代码如下所示
 *
 * .groupBy(new Fields("cityDiseaseHour")).persistentAffregate(new OutbreakTreadFactory().new Count(),new Fields("count").newValueStream)
 * groupBy()方法强制数据重新分片，将特定字段值相同的tuple分组到同一个分片中，所以 ，storn必须将相似的tuple发送到相同的主机上，
 *
 */
public class Count implements CombinerAggregator<Long> {

    public Long init(TridentTuple tridentTuple) {
        return 1L;
    }

    public Long combine(Long val1, Long val2) {
        return val1+val2;
    }

    public Long zero() {
        return 0L;
    }
}
