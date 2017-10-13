package cn.spark.studt.streaming.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created by Zouyy on 2017/9/22.
 */
public class ConnectionUtil {

    private static LinkedList<Connection> connectionsQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接,多线程访问并发控制
     * @return
     */
    public synchronized static Connection getConnection() {
        if (connectionsQueue == null) {
            connectionsQueue = new LinkedList<Connection>();
            for (int i = 0; i < 10; i++) {
                try {
                    Connection connection = DriverManager.getConnection(
                            "jdbc:mysql://hadoop7:3306/mysql",
                            "root",
                            "Mysql123,./"
                    );
                    connectionsQueue.push(connection);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return connectionsQueue.poll();
    }

    /**
     * 放回去一个连接
     * @param conn
     */
    public static void returnConnection(Connection conn){
        connectionsQueue.push(conn);
    }

}
