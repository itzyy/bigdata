package cn.kepuchina.trident.function;

import cn.kepuchina.trident.filter.DiseaseFilter;
import cn.kepuchina.trident.spout.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Zouyy on 2017/8/9.
 *
 */
public class CityAssignment extends storm.trident.operation.BaseFunction {

    private static final long serialVersionUID =1l;
    private static final Logger LOG =  LoggerFactory.getLogger(DiseaseFilter.class);

    private static Map<String,double[]> CITLES = new HashMap<String, double[]>();

    {

        double[] phl = {39.875365,-75.249524};
        CITLES.put("PHL",phl);
        double[] nyc = {40.71448,-74.00598};
        CITLES.put("NTC",nyc);
        double[] sf = {-31.05374,-62.0841809};
        CITLES.put("SF",sf);
        double[] la ={-34.05374,-118.24307};
        CITLES.put("LA",la);
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent)tuple.getValue(0);
        double leastDistance = Double.MAX_VALUE;
        String closestCity = "NONE";

        //查询最靠近的城市
        for (Map.Entry<String,double[]> city:CITLES.entrySet()) {
            double r = 6371;
            double x = (city.getValue()[0]-diagnosis.lng)*Math.cos(city.getValue()[0]+diagnosis.lng)/2;
            double y = city.getValue()[1] - diagnosis.lat;
            double d = Math.sqrt(x * x + y * y) * r;
            if(d<leastDistance){
                leastDistance = d;
                closestCity = city.getKey();
            }
        }

        //发射数据
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(closestCity);
        LOG.debug("Closest city to lat=["+diagnosis.lat+"],lng=["+diagnosis.lng+"]==["+closestCity+"],d=["+leastDistance+"]");
        /**
         * funcation发送数据时，将新字段添加到tuple中，并不会删除或者变更已有的字段
         * function声明的字段数量必须和他发射出值的字段数一数，如果不一致，storm就会抛出IndexOutBoundException
         */

        collector.emit(values);
    }
}
