package cn.kepuchina.trident.factory;

import backtype.storm.task.IMetricsContext;
import cn.kepuchina.trident.state.OutbreakTreandState;
import cn.kepuchina.trident.state.OutbreakTrendBackingMap;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class OutbreakTreandFactory implements StateFactory {


    private static final long serialVersionUID=1l;


    /**
     * 返回一个stat对象
     * @param map
     * @param iMetricsContext
     * @param partitionIndex
     * @param numPartitions
     * @return
     */
    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        return new OutbreakTreandState(new OutbreakTrendBackingMap());
    }
}
