package cn.kepuchina.trident.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * Created by Zouyy on 2017/8/9.
 */
public class DiagnosisEventSpout implements ITridentSpout<Long> {

    private static final long serialVersionUID =1L;

    SpoutOutputCollector collector;

    BatchCoordinator<Long>  coordinator = new DefaultCoordinator() ;

    Emitter<Long> emitter = new DiagnosisEventEmitter();

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
        return new Fields("event");
    }
}
