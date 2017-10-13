package cn.kepuchina.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.sun.xml.internal.ws.api.ha.StickyFeature;

/**
 * Created by Zouyy on 2017/8/8.
 */
public class WordCountTopology {


    /**
     *
     * Storm OutbreakDetectionTopology 通常有Java的main()函数来定义，运行或者提交（部署到集群的操作）
     *
     * 首先，我们定义了一系列的字符串常量，作为storm组件的唯一标识符，m
     * main方法中，首选实例了spout和bolt，并生成一个topologyBuilder实例。TopologyBuilder类提供了流失接口风格的API来定义topology组件之间的
     * 数据流，
     *
     *
     *
     * @param args
     */


    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_id = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-OutbreakDetectionTopology";
;
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountTopology = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();
        TopologyBuilder builder = new TopologyBuilder();

        // 首先，注册一个sentence spout并且赋值给其唯一的ID；
        builder.setSpout(SENTENCE_SPOUT_ID,spout);
        //        builder.setSpout(SENTENCE_SPOUT_ID,spout,2) 设置一个组件指派的executor的数据
        /**
         *  然后注册一个SplitSentenceBolt，这个订阅SentenceSpout发射出来的数据流；
         *  类TopologyBuilder的setBolt()方法会注册一个Bolt,并且返回BoltDeclarer的实例，可以定义bolt的数据源。
         *  将sentenceSpout的唯一ID赋值给shuffleGrouping方法确认了这种订阅关系
         *  shuffleGrouping方法告诉storm，要将类sentenceSpout发射的tuple随机均匀的分发给splitSentenceBolt的实例
         */
        builder.setBolt(SPLIT_BOLT_ID,splitSentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        //        builder.setBolt(SPLIT_BOLT_ID,splitSentenceBolt,2).setNumTasks(4); 设置2个execute对应4个task
        /**
         * 建立了SplitSentenceBolt和WordCountBolt之间的连接关系
         * 通过fieldsGrouping()方法保证所有“word”字段值相同的tuple会被路由到同一个WordCountBolt实例中
         */
        builder.setBolt(COUNT_BOLT_id,wordCountTopology).fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));
        /**
         *通过globalGrouping 将WordCountBolt实例发射出的tuple流路由到唯一的ReportBolt任务中
         */
        builder.setBolt(REPORT_BOLT_ID,reportBolt).globalGrouping(COUNT_BOLT_id);

        //配置好数据流之后，进行便宜提交的集群上面

        /**
         *  当一个topology提交后，Storm会将默认配置和Config实例中的配置合并后作为参数传递给submitTopology.
         *  Config对象代表了对topology所有组件全局生效的配置参数集合。
         */
        Config config = new Config();

        //        config.setNumWorkers(2);增加一个worker数量

        //使用storm上面的LocalCluster在本地开发环境来模拟一个完整的Storm集群
//        LocalCluster cluster = new LocalCluster();

        /**
         * 提交一个topology到本地集群
         *  当一个topology提交后，Storm会将默认配置和Config实例中的配置合并后作为参数传递给submitTopology.
         *  合并后的配置被分发个各个spout的bolt的open()、prepare()方法。
         */
//        cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
//        StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology()); 提交一个topology到远程集群


        //根据参数判断使用本地模式还是集群模式
        if(args.length==0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(args[0],config,builder.createTopology());
            Thread.sleep(20000);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }else{
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

//        Thread.sleep(20000);
//        cluster.killTopology(TOPOLOGY_NAME);
//        cluster.shutdown();


    }

}
