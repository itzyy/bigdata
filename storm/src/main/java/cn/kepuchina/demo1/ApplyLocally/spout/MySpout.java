package cn.kepuchina.demo1.ApplyLocally.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * Created by Zouyy on 2017/8/9.
 * 泛型类存放一个batch所需要的元数据，batchCoordinator作为一个StormBolt运行在一个单线程中，Storm会在Zookeeper中持久化存储这个元数据。
 * 当事务处理完成时会通知到对应的coordinator
 */
public class MySpout implements ITridentSpout<Long> {

    private static final long serialVersionUID =1L;

    SpoutOutputCollector collector;

    BatchCoordinator<Long>  coordinator = new MyCoordinator() ;

    Emitter<Long> emitter = new MyEmitter();

    /**
     * 负责管理批次和元数据
     * spout发射一个字段event,为DiagnsisEvent类
     *
     * @param s
     * @param map
     * @param topologyContext
     * @return
     */
    public BatchCoordinator getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return coordinator;
    }

    /**
     * Emitter负责发送tuple
     * @param s
     * @param map
     * @param topologyContext
     * @return
     */
    public Emitter getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    public Map getComponentConfiguration() {
        return null;
    }

    /**
     * 声明要发射的tuple包括哪些字段
     * @return
     */
    public Fields getOutputFields() {
        return new Fields("a");
    }
}
