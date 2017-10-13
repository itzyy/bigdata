package cn.kepuchina.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by LENOVO on 2017/8/8.
 *
 * 定义一个topology，实现单词计数的组件
 * 1.在bolt中的perpare()方法中，实例化了一个HashMap 用来存储单词和对应的计数 ,
 * 大部分实例变量都在这个方法中进行实例化，这个设计模式有topology的部署方式决定。
 * 当topology发布时，所有的bolt和spout组件首先会进行序列化，然后通过网络发送到集群中。如果spout或bolt在序列化之前（比如在构造函数中生长）
 * 实例化了任何无法序列化的实例变量，在进行序列化时会抛出NoSerializableException异常，topology就是部署失败
 * 通常情况下最好是在构造函数中对基本数据类和可序列化的对象进行赋值和实例化，在prepare()中对不可序列化的对象进行序列化
 */
public class WordCountBolt extends BaseRichBolt {


    private OutputCollector collector;

    private HashMap<String,Long> counts ;


    /**
     *
     * @param map
     * @param topologyContext   提供了topolopy中组件的信息，
     * @param outputCollector
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String, Long>();
    }

    /**
     * 当接受到一个单词时，首先查找到这个单词对应的计数（如果单词没有出现过则计数初始化为0），递增并存储计数，然后将单词和最新计数作为tuple向后发射。
     * 将单词计数作为数据流发射，topology中的其他toply就可以订阅这个数据流进行进一步的处理
     * @param tuple
     */
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count==null){
            count=0L;
        }
        count++;
        this.counts.put(word,count);
        this.collector.emit(new Values(word,count));
    }

    /**
     * 声明一个输出刘，其中tuple包含单词和对应的计数
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }
}
