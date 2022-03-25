package com.atguigu.cdc;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author shkstart
 * @create 2022-03-14 19:44
 */
public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE user_info (" +
                "  id INT primary key," +
                "  name STRING," +
                "  phone_num STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'hadoop102'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = '123456'," +
                " 'scan.incremental.snapshot.enabled' = 'false', " +
                "  'database-name' = 'gmall210927'," +
                "  'table-name' = 'user_info'" +
                ")");
        tableEnv.executeSql("select * from user_info").print();

        env.execute();

    }
}
