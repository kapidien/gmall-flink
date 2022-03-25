package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserJumpStats;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @create 2022-03-21 11:13
 */
public class UserJumpVisitDim10sApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_page_log";
        String groupId  = "userJumpVisitDim10sApp";
        //todo 从kafak读取数据
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtils.getKafkaStream(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObDS = streamSource.map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = jsonObDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));
        //todo 按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //todo 定义模式

        Pattern<JSONObject, JSONObject> within = Pattern.<JSONObject>begin("start").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).next("next").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).within(Time.seconds(10));

        //todo 将模式作用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedStream, within);

        //todo 提取匹配事件和超时事件

        OutputTag<JSONObject> timeout = new OutputTag<JSONObject>("timeout") {
        };

        SingleOutputStreamOperator<JSONObject> selectDS = patternDS.select(timeout, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                return pattern.get("Start").get(0);
            }
        });

        DataStream<JSONObject> timeoutDS = selectDS.getSideOutput(timeout);

//        selectDS.print("selectDS>>>");
//        timeoutDS.print("timeoutDS>>>");

        DataStream<JSONObject> unionDS = selectDS.union(timeoutDS);
        //将数据转化成Javabean
        SingleOutputStreamOperator<UserJumpStats> userJumpStatsDS = unionDS.map(json -> {
            JSONObject common = json.getJSONObject("common");
            return new UserJumpStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    json.getLong("ts"));
        });
//        userJumpStatsDS.print("javabean");

        //分组开窗聚合
        KeyedStream<UserJumpStats, Tuple4<String, String, String, String>> tuple4KeyedStream = userJumpStatsDS.keyBy(new KeySelector<UserJumpStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(UserJumpStats value) throws Exception {
                return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
            }
        });


        SingleOutputStreamOperator<UserJumpStats> reduce = tuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<UserJumpStats>() {
            @Override
            public UserJumpStats reduce(UserJumpStats value1, UserJumpStats value2) throws Exception {
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                return value1;
            }
        }, new WindowFunction<UserJumpStats, UserJumpStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<UserJumpStats> input, Collector<UserJumpStats> out) throws Exception {
                UserJumpStats userJumpStats = input.iterator().next();

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                userJumpStats.setStt(dateFormat.format(window.getStart()));
                userJumpStats.setEdt(dateFormat.format(window.getEnd()));
                out.collect(userJumpStats);
            }
        });

        //将数据写入到ck

        reduce.print("result>>>>>>>>>>>>>>");
        reduce.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_userjump_vc_ch_isnew_ar_10s values(?,?,?,?,?,?,?,?)"));


        env.execute("UserJumpVisitDim10sApp");


    }
}
