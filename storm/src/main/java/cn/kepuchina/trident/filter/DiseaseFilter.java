package cn.kepuchina.trident.filter;

import cn.kepuchina.trident.spout.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;


/**
 * Created by Zouyy on 2017/8/9.
 *
 * 使用方法：inputStream.each(new Fields("event",new DiseaseFilter))
 */
public class DiseaseFilter extends BaseFilter {


    private static final long serialVersionUID = 1l;
    private static final Logger LOG =  LoggerFactory.getLogger(DiseaseFilter.class);

    public boolean isKeep(TridentTuple tridentTuple) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.get(0);

        Integer code = Integer.parseInt(diagnosis.diagnosisCode);

        if(code.intValue()<=322){
            LOG.debug("Emitting disease ["+diagnosis.diagnosisCode+"]");
            //将tuple发送到下游进行操作
            return true;
        }else{
            LOG.debug("Filtering disease ["+diagnosis.diagnosisCode+"]");
            return false;
        }
    }
}
