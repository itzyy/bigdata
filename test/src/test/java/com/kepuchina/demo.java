package com.kepuchina;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Zouyy on 2017/9/1.
 */
public class demo {


    @Test
    public void test1(){
        System.out.println("这是我的第一个java小程序！");
    }

    /**
     * 声明变量
     */
    @Test
    public void test2(){

        String str="您好，小明";
        System.out.println(str);
        int age =18;
        System.out.println(str+"，欢迎你进入"+age+"岁");
        String[] userinfo={"鸡小萌","邹蕴玉","高晓松"};
        System.out.println("欢迎新同学"+userinfo[0]+"\t"+userinfo[1]+"\t"+userinfo[2]);
        List list = new ArrayList();
        list.add("星期一");
        list.add("星期二");
        list.add("星期三");
        System.out.println(list.get(0));
        list.remove(0);
        System.out.println(list.get(0));
        list.add("星期一");
        System.out.println(list.get(0));
        Map map = new HashMap();
        map.put("123","湖北");
        map.put("456","湖南");
        System.out.println(map.get("123"));

    }

}
