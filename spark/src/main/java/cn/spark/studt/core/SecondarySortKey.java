package cn.spark.studt.core;

import scala.Serializable;
import scala.math.Ordered;

/**
 * Created by Zouyy on 2017/9/8.
 */
public class SecondarySortKey implements Serializable, Ordered<SecondarySortKey> {

    private int first;
    private int second;


    public SecondarySortKey(int first, int second) {

        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $less(SecondarySortKey secondarySortKey) {
        if (this.first < secondarySortKey.getFirst()) {
            return true;
        } else if (this.first == secondarySortKey.getFirst() && this.second < secondarySortKey.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    /**
     * 当前这个key和其他key相比,是大于其他key的
     */
    public boolean $greater(SecondarySortKey other) {
        if (this.first > other.getFirst()) {
            return true;
        } else if (this.first == other.getFirst()
                && this.second > other.getSecond()
                ) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if (this.$less(other)) {
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey secondarySortKey) {
        if (this.$greater(secondarySortKey)) {
            return true;
        } else if (this.first == secondarySortKey.getFirst() && this.second == secondarySortKey.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondarySortKey secondarySortKey) {
        if(this.first-secondarySortKey.getFirst()!=0){
            return this.first-secondarySortKey.getFirst();
        }else{
            return this.second-secondarySortKey.getSecond();
        }
    }

    @Override
    public int compare(SecondarySortKey secondarySortKey) {
        if(this.first-secondarySortKey.getFirst()!=0){
            return this.first-secondarySortKey.getFirst();
        }else{
            return this.second-secondarySortKey.getSecond();
        }
    }

    //为要进行排序的多个列,提供gettter和setter方法,并且生成hashcode方法和equals方法


    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }
}
