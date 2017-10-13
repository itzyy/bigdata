package cn.kepuchina.demo1.ApplyLocally;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import cn.kepuchina.demo1.ApplyLocally.aggrprivate.MyEcho;
import cn.kepuchina.demo1.ApplyLocally.aggrprivate.MyFunctiona;
import cn.kepuchina.demo1.ApplyLocally.spout.MySpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class ApplyLocallyTopology {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc",config,buildTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }

    public static StormTopology buildTopology(){

        TridentTopology topology = new TridentTopology();
        MySpout spout =new MySpout();
        Stream stream = topology.newStream("local", spout);
        stream.each(
                new Fields("a"),
                new MyFunctiona(),
                new Fields("d")
        ).each(
                new Fields("a","d"),
                new MyEcho(),
                new Fields()
        );
        return topology.build();
    }
}
