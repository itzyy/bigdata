package cn.kepuchina.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by Zouyy on 2017/8/8
 *
 * 作用：对所有单词的计数生成一份报告，和WordCountBolt类似。
 * ReportBolt使用一个HashMap<String,Long> 对象来保存单词和对应计数。本例中，他的功能是简单的存储接收到计数bolt发射出的计数tuple
 *
 * 上报bolt和上述其他bolt的一个区别是，他是一个位于数据流末端的bolt,只接收tuple。因为他不发射任何数据流，所以deaclareOutputFields()方法是空的
 * 上报bolt中初次引入了cleanup()方法，这个方法在IBolt接口中定义。Storm在终止一个bolt之前会调用这个方法。
 * 本例中我们利用cleanup()方法在topology关闭时输出最终的计数结果。通常情况下，cleanip()方法用来释放bolt占用的资源，如打开的文件句柄或数据库连接。
 * 注意：当topology在storm集群上运行时，Ibolt.cleanup()方法是不可靠的，不能保证会执行。
 */
public class ReportBolt extends BaseRichBolt {

    private HashMap<String ,Long> counts;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word,count);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("-------------FINAL COUNTS---------------");
        ArrayList<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key:
             keys) {
            System.out.println(key+"："+this.counts.get(key));
        }
        System.out.println("----------------------------------------");
    }
}
