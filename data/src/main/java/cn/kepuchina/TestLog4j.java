package cn.kepuchina;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Zouyy on 2017/8/11.
 */
public class TestLog4j {

    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger("flume");

        while(true){
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm;ss");
            String nowdate = format.format(new Date());
            logger.info("当前时间戳：{}",nowdate);
            Thread.sleep(1000);
        }
    }

}
