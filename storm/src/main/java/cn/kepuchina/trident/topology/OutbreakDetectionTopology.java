package cn.kepuchina.trident.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import cn.kepuchina.trident.Aggregator.Count;
import cn.kepuchina.trident.factory.OutbreakTreandFactory;
import cn.kepuchina.trident.filter.DiseaseFilter;
import cn.kepuchina.trident.function.CityAssignment;
import cn.kepuchina.trident.function.DispatchAlert;
import cn.kepuchina.trident.function.HourAssignment;
import cn.kepuchina.trident.function.OutbreakDetector;
import cn.kepuchina.trident.spout.DiagnosisEventSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class OutbreakDetectionTopology {



    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc",config,buildTopology());
        Thread.sleep(200000);
        cluster.killTopology("cdc");
        cluster.shutdown();
    }

    public static StormTopology buildTopology(){
        TridentTopology topology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = topology.newStream("event", spout);
        inputStream
                .each(new Fields("event"),new DiseaseFilter())
                .each(new Fields("event"),new CityAssignment(),new Fields("city"))
                .each(new Fields("event","city"),new HourAssignment(),new Fields("hour","cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(
                        new OutbreakTreandFactory(),
                        new Count(),
                        new Fields("count")
                )
                .newValuesStream()
                .each(
                        new Fields("cityDiseaseHour","count"),
                        new OutbreakDetector(),
                        new Fields("alert")
                )
                .each(
                        new Fields("alert"),
                        new DispatchAlert(),
                        new Fields()
                );
        return topology.build();
    }

}
