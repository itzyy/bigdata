package cn.kepuchina.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by LENOVO on 2017/8/4.
 */
public class SimplePartition implements Partitioner {
    private VerifiableProperties verifiableProperties;

    public SimplePartition(VerifiableProperties verifiableProperties) {
        this.verifiableProperties = verifiableProperties;
    }

    public int partition(Object key, int numPartitions) {

        String sKey = String.valueOf(key);
        System.out.println("============================================"+sKey);
        System.out.println(numPartitions);
        try {
            long partitionNum = Long.parseLong((String) key);
            return (int) Math.abs(partitionNum % numPartitions);
        } catch (Exception e) {
            e.printStackTrace();
            return Math.abs(key.hashCode() % numPartitions);

        }
    }
}

