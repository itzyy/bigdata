package cn.kepuchina.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by LENOVO on 2017/8/8
 *
 * 创建一个Bolt
 * Bolt可以理解为程序中的运算或者函数，将一个或者多个数据流作为输入，对数据实现运算后，选择性地输出一个或者多个数据流。bolt可以订阅多个
 * 多个有spout或者其他bolt发射的数据流，这样就可以建立复杂的数据流转换网络
 *
 *  实现语句分割.
 *  BaseRichBolt是ICompent和IBolt接口的简便实现。继承这个类，就不用去实现本例不关心的方法，将注意力放在实现我们需要的功能上。
 */
public class SplitSentenceBolt extends BaseRichBolt {


    private OutputCollector collector;

    /**
     * perpare() 在IBolt中定义，类同于ISpout接口中定义的open()方法。在所有bolt初始化时调用，可以用来准备bolt的资源，如数据库连接
     * @param map   storm配置信息的map，
     * @param topologyContext   提供了topolopy中组件的信息，
     * @param outputCollector   提供了发射tuple的方法
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector  = outputCollector;
    }


    /**
     * 核心功能。这个方法是IBolt接口定义的，每当从订阅的数据流中接受一个tuple,都会调用这个方法。
     * 本例中，execute()方法按照字符串读取sentence字段的值，然后将其拆分为单词，每个单词向后面的输出流发射一个tuple
     * @param tuple
     */
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word:words) {
            this.collector.emit(new Values(word));
//            this.collector.emit(tuple,new Values(word)); 建立和读入的tuple之间的关系，生成tuple树
        }
    }

    /**
     * 声明一个输出刘，每个tuple包含一个字段“word”
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
