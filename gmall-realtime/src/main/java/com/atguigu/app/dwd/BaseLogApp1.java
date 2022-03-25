package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
 * @create 2022-03-17 11:01
 */
public class BaseLogApp1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> topicLogDS = env.addSource(MyKafkaUtils.getKafkaStream("topic_log", "220410"));

        //清洗数据将脏数据写到侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty>>>") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObDS = topicLogDS.process(new ProcessFunction<String, JSONObject>() {
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

        KeyedStream<JSONObject, String> keyedStream = jsonObDS.keyBy(jsonOb -> jsonOb.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> map = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> newUser;

            @Override
            public void open(Configuration parameters) throws Exception {
                newUser = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNew", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNewValue = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNewValue)) {
                    if (newUser.value() != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                newUser.update("0");
                return value;
            }
        });

        //将数据分流
        OutputTag<String> displayTag = new OutputTag<String>("display>>>") {};
        OutputTag<String> errorTag = new OutputTag<String>("error>>>") {};
        OutputTag<String> actionTag = new OutputTag<String>("action>>>") {};
        OutputTag<String> startTag = new OutputTag<String>("start>>>") {};

        SingleOutputStreamOperator<String> pageDS = map.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                if (value.getString("err") != null) {
                    ctx.output(errorTag, value.toJSONString());
                }else if (value.getString(("start")) != null) {
                    ctx.output(startTag, value.toJSONString());
                }else {
                    out.collect(value.toJSONString());

                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("ts", value.getLong("ts"));
                            display.put("page_id", value.getJSONObject("page").getString("page_id"));
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", value.getJSONObject("common"));
                            action.put("page_id", value.getJSONObject("page").getString("page_id"));
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }
                }


            }
        });
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionTDS = pageDS.getSideOutput(actionTag);

        pageDS.print("page>>>>");
        startDS.print("start>>>>");
        errorDS.print("error>>>");
        displayDS.print("display>>>");
        actionTDS.print("action>>>");



        pageDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_display_log"));
        actionTDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_action_log"));
        errorDS.addSink(MyKafkaUtils.getKafkaProducer("dwd_error_log"));

        env.execute();


    }
}
