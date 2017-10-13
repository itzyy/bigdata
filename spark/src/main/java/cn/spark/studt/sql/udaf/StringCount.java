package cn.spark.studt.sql.udaf;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Created by Zouyy on 2017/9/19.
 */
public class StringCount extends UserDefinedAggregateFunction {
    @Override
    /**
     * 指的是输入的数据类型
     */
    public StructType inputSchema() {
        List<StructField> fields = Lists.newArrayList();
        fields.add(DataTypes.createStructField("str",DataTypes.StringType,true));
        return DataTypes.createStructType(fields);
    }

    @Override
    /**
     * 指的是中间进行聚合，所处理的数据类型
     */
    public StructType bufferSchema() {
        List<StructField> fields = Lists.newArrayList();
        fields.add(DataTypes.createStructField("count", DataTypes.IntegerType,true));
        return DataTypes.createStructType(fields);
    }

    @Override
    /**
     * 函数返回值的类型
     */
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    /**
     * 一致性校验，如果为true，那么输入不变的情况下计算的结果是不变的。
     */
    public boolean deterministic() {
        return true;
    }

    @Override
    /**
     * 设置聚合中间buffer的初始值，但需要保证这个语义：两个初始buffer调用下面实现的merge方法后应该为初始buffer
     * 即如果你初始值是1，然后你merge是执行一个想加的动作，两个初始buffer合并之后等于2.
     * 不会等于初始buffer了，这样的初始值就是有问题的，所以初始值也叫“zero value”
     */
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0);
    }

    @Override
    /**
     * 用输入数据input更新buffer值，类似于combineByKey
     */
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0,Integer.valueOf(buffer.getAs(0).toString())+1);
    }

    @Override
    /**
     * 合并两个buffer，将buffer2合并到buffer1,在合并两个分区聚合结果的时候会被用到，类似与reduceByKey
     * 这里要注意该方法没有返回值，在实现的时候是把buffer2合并到buffer1中去，你需要实现这个合并调节
     */
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0,Integer.valueOf(buffer1.getAs(0).toString())+Integer.valueOf(buffer2.getAs(0).toString()));
    }

    @Override
    /**
     * 计算并返回最终的聚合结果
     */
    public Object evaluate(Row buffer) {
        return buffer.getInt(0);
    }
}
