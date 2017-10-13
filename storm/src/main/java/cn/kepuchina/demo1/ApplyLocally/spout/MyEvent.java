package cn.kepuchina.demo1.ApplyLocally.spout;

/**
 * Created by Zouyy on 2017/8/10.
 */
public class MyEvent {

    private int segment1;
    private int segment2;
    private int segment3;

    public MyEvent(int segment1, int segment2, int segment3) {
        this.segment1 = segment1;
        this.segment2 = segment2;
        this.segment3 = segment3;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s",segment1,segment2,segment3);
    }
}
