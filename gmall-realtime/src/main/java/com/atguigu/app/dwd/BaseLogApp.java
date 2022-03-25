package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author shkstart
 * @create 2022-03-12 20:44
 */
//数据流： web/app -> Nginx -> 日志服务器(xx.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序： Mock() -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","atguigu");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(6000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkcdc/210927"));

//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/210927");
        //从kafka读取数据

        DataStreamSource<String> topicLog = env.addSource(MyKafkaUtils.getKafkaStream("topic_log", "210927"));

        OutputTag<String> outputTag = new OutputTag<String>("notJson"){};
        //数据清洗，将脏数据写到侧输出流
        SingleOutputStreamOperator<JSONObject> process = topicLog.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });

        process.getSideOutput(outputTag).print("dirty>>>>");

        KeyedStream<JSONObject, String> keyedStream = process.keyBy(jsonObject -> jsonObject.getObject("common", JSONObject.class).getString("mid"));



        //判断isNew
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> isNewState;

            @Override
            public void open(Configuration parameters) throws Exception {
                isNewState = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNewSt", String.class));

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String value_is_new = value.getObject("common", JSONObject.class).getString("is_new");

               //先看是否为1
                if ("1".equals(value_is_new)) {
                    if (isNewState.value() != null) {
                        value.getObject("common", JSONObject.class).put("is_new", 0);
                    }
                }
                //无论怎样都更新来过就是老用户了
                isNewState.update("0");
                return value;
            }
        });

        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> startTag = new OutputTag<String>("start"){};

        //TODO 5.分5个流(侧输出流)
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                if (value.getString("err") != null) {
                    ctx.output(errorTag, value.toJSONString());
                }
                if (value.getString("start") != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //页面日志去主流
                    out.collect(value.toJSONString());
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");

                    //行为日志
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject actionsJSONObject = actions.getJSONObject(i);
                            actionsJSONObject.put("common", value.getJSONObject("common").toJSONString());
                            actionsJSONObject.put("page_id",pageId);
                            ctx.output(actionTag, actionsJSONObject.toJSONString());
                        }
                    }
                    //浏览行为
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("ts",ts);
                            display.put("page_id",pageId);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }

            }
        });

        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);

        //打印测试
        pageDS.print("pageDS>>>>>");
        errorDS.print("errorDS>>>>");
        displayDS.print("displayDS>>>>");
        actionDS.print("actionDS>>>");
        startDS.print("startDS>>>>>");

        pageDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_page_log"));
        errorDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_error_log"));
        displayDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_display_log"));
        actionDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_action_log"));
        startDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_start_log"));

        env.execute();

    }
}
