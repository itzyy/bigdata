package cn.kepuchina.trident.function;

import cn.kepuchina.trident.filter.DiseaseFilter;
import cn.kepuchina.trident.spout.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;

/**
 * Created by Zouyy on 2017/8/9.
 * 转化UNIX时间戳为纪元时间的小时，可以用来时间发生进行时间上的分组操作
 */
public class HourAssignment extends BaseFunction {

    private static final long serialVersionUID=1l;
    private static final Logger LOG =  LoggerFactory.getLogger(DiseaseFilter.class);

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        DiagnosisEvent diagnosis = (DiagnosisEvent)tridentTuple.getValue(0);
        String city = (String)tridentTuple.getValue(1);


        long time = diagnosis.time;

        long hourSinceEpoch = time / 1000 / 60 / 60;

        LOG.debug("key = ["+city+":"+hourSinceEpoch+"]");
        String key = city + ":" + diagnosis.diagnosisCode + ":" + hourSinceEpoch;
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(hourSinceEpoch);
        values.add(key);
        tridentCollector.emit(values);
    }
}
