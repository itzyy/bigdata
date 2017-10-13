package cn.kepuchina.trident.state;

import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class OutbreakTreandState extends NonTransactionalMap<Long> {
    public OutbreakTreandState(OutbreakTrendBackingMap backing) {
        super(backing);
    }
}
