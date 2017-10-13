package cn.kepuchina.trident.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;

import java.io.Serializable;

/**
 * Created by Zouyy on 2017/8/9.
 * BatchCoordinator是一个泛型类，这个类是重放batch所需要的元数据。在本例中，spout发送随机时间，因此元数据可以忽略，实际系统中，元
 * 数据可能包含组成了这个batch的消息或者对象的标识符。通过这个信息，非透明型和事务型spout可以约定，确保batch的内容不会出现重复，
 * 在事务型spout中，batch的内容不会出现变化
 * BatchCoordinator类作为一个Storm Bolt运行在一个单线程中，Storm会在Zookeeper中持久化存储这个元数据，当事务处理完成时会通知到
 * 对应的coordinator.
 */
public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>,Serializable {

    private static final long serialVersionUID=1L;

    private static  final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);
    public Long initializeTransaction(long txid, Long aLong, Long x1) {
        LOG.info(("Initializing Transaction ["+txid+"]"));
        return null;
    }

    public void success(long l) {
        LOG.info(("Initializing Transaction ["+l+"]"));
    }

    public boolean isReady(long l) {
        return true;
    }

    public void close() {

    }
}
