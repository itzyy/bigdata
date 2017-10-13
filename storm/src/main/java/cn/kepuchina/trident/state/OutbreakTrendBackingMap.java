package cn.kepuchina.trident.state;

import cn.kepuchina.trident.filter.DiseaseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class OutbreakTrendBackingMap implements IBackingMap<Long> {


    private static final Logger LOG =  LoggerFactory.getLogger(DiseaseFilter.class);

    private Map<String,Long> storage = new ConcurrentHashMap<String, Long>();

    public List<Long> multiGet(List<List<Object>> list) {
        ArrayList<Long> values = new ArrayList<Long>();
        for (List<Object> key : list){

             Long value =  storage.get(key.get(0));

             if(value == null){
                values.add(new Long(0));
             }else{
                 values.add(value);
             }
        }
        return values;

    }

    public void multiPut(List<List<Object>> keys, List<Long> vals) {
        for (int i = 0; i < keys.size(); i++) {
            LOG.info("Persistin ["+keys.get(i).get(0)+"] ==> ["+vals.get(i)+"]");
            storage.put((String) keys.get(i).get(0),vals.get(i));
        }
    }
}
