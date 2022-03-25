package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author shkstart
 * @create 2022-03-16 18:04
 */
public class FlinkSQLJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));


        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                    @Override
                    public long extractTimestamp(Bean1 element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean2(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        tableEnv.createTemporaryView("b1",bean1DS);
        tableEnv.createTemporaryView("b2",bean2DS);

        tableEnv.sqlQuery("select b1.id,b2.id,b1.name,b2.sex from b1 join b2 on b1.id = b2.id").execute().print();



        env.execute();


    }
}
