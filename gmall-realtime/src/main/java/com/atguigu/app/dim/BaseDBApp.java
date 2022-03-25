package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.MySinkFunction;
import com.atguigu.app.func.MyStringDebeziumDeserializationSchema;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.app.func.TableProcessFunction1;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtils;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author shkstart
 * @create 2022-03-15 15:06
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境设置为Kafka分区数

        //        //1.1 开启CheckPoint
        //        env.enableCheckpointing(5 * 60000L);
        //        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //
        //        //1.2 指定状态后端
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/xxxx/xxx"));

        //TODO 2.读取Kafka topic_db主题数据创建流
        DataStreamSource<String> topicDBDS = env.addSource(MyKafkaUtils.getKafkaStream("topic_db", "topic_db"));

        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        //TODO 3.过滤脏数据并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = topicDBDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        //TODO 4.使用FlinkCDC读取MySQL配置信息表创建配置流
        DebeziumSourceFunction<String> debeziumSourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210927-realtime")
                .tableList("gmall-210927-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(debeziumSourceFunction);


        //TODO 5.将配置流做成广播流

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 6.将主流与广播流连接

        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastDS);


        //TODO 7.根据广播流数据处理主流数据

        SingleOutputStreamOperator<JSONObject> process = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        process.print();

        //TODO 8.将处理后的数据写入Phoenix

        process.addSink(new MySinkFunction());

        //TODO 9.启动任务
        env.execute();






    }
}
