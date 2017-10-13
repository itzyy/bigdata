package cn.kepuchina.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LENOVO on 2017/8/8.
 *
 * 创建一个Spout
 * Spout代表一个Storm topology的主要数据入口，充当了采集器的角色，连接到数据源，将数据转化成一个个tuple,并将tuple作为数据流进行发射。
 * 主要工作：是从数据源或API中消费数据
 * 因为Spout通常不会用来实现业务逻辑，所以在多个topology中常常可以复用
 */
public class SentenceSpout extends BaseRichSpout{

    /*
    *
    * BaseRichSpout类是ISpout接口和ICompoent接口的简便实现。使用这个类，我们可以专注在所需要的方法上
    *
    * */
    private int index =0;

    private ConcurrentHashMap<UUID,Values> pending;

    private SpoutOutputCollector collector;

    private String[] sentences ={
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas",
            "but the is good"
    };

    /**
     * 接收三个参数，如下，在所有的Sput组件在初始化石嗲用这个方法
     * 本例，只是仅仅简单讲SpoutOuputColler对象的引用保存在变量中
     * @param map   包含了storm配置信息的map，
     * @param topologyContext   提供了topolopy中组件的信息，
     * @param spoutOutputCollector  提供了发射tuple的方法
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    /**
     * spout实现的核心所在，Storm通过调用这个方法向输出的collector发射tuple
     *  这个例子中，我们发射当前索引对应的语句，并且递增索引指向下一个语句
     */
    public void nextTuple() {
        UUID uuid = UUID.randomUUID();
        this.collector.emit(new Values(sentences[index]));
//        this.collector.emit(new Values(sentences[index]),uuid); 添加tupleid，一是为了标记tuple，二是为了锚定读入tuple和衍生的tuple之间的关系
//        this,collector.emit(new Values(sentences[index]),"msgid"); 给每个发出的tuple都带上唯一的ID,并且将ID作为参数传递给SpoutOutputCollector的emit方法

        //将发送的信息保存到HashMap中，发送成功调用ackremove，发送失败调用fail者重新发送
        this.pending.put(uuid,new Values(sentences[index]));
        index++;
        if (index>=sentences.length){
            index =0;
        }
    }

    /**
     * 见应答者（ack）方法，发送成功后，调用此方法
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId),msgId);
    }

    /**
     * 是在IComponent接口中固定翼的，所有Storm的组件（Spout和bolt）都需要实现这个接口
     * 通过这个方法告诉Storm该组件会发射哪些数据流，没个数据流的tuple中包含哪些字段。
     * 本例中，发射一个数据流，其中tuple包含一个字段（）
     * @param outputFieldsDeclarer  输出信息声明对象，将声明信息装载在这个类中
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
