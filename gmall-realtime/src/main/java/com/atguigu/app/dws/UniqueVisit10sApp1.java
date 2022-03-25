package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UniqueVisitStats;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author shkstart
 * @create 2022-03-20 11:37
 */
public class UniqueVisit10sApp1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_page_log";
        String groupId  = "UniqueVisit10sApp1";

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtils.getKafkaStream(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonDS = streamSource.map(JSONObject::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //根据mid进行去重
        SingleOutputStreamOperator<JSONObject> filter = jsonDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lasttate", String.class));

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                JSONObject page = value.getJSONObject("page");
                String lastDate = lastDateState.value();
                if (page.getString("last_page_id") == null) {

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    String currentdate = sdf.format(value.getLong("ts"));
                    if (lastDate == null || !lastDate.equals(currentdate)) {
                        return true;
                    }
                    return false;
                }

                return false;
            }
        });

        //todo 转换成JavaBean分组开窗聚合写入
        SingleOutputStreamOperator<UniqueVisitStats> resultDS = filter.map(new MapFunction<JSONObject, UniqueVisitStats>() {
            @Override
            public UniqueVisitStats map(JSONObject value) throws Exception {
                JSONObject common = value.getJSONObject("common");

                return new UniqueVisitStats("", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L,
                        value.getLong("ts"));
            }
        }).keyBy(new KeySelector<UniqueVisitStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(UniqueVisitStats value) throws Exception {

                return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<UniqueVisitStats>() {
            @Override
            public UniqueVisitStats reduce(UniqueVisitStats value1, UniqueVisitStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                return value1;
            }
        }, new WindowFunction<UniqueVisitStats, UniqueVisitStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<UniqueVisitStats> input, Collector<UniqueVisitStats> out) throws Exception {

                UniqueVisitStats next = input.iterator().next();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                next.setStt(simpleDateFormat.format(window.getStart()));
                next.setEdt(simpleDateFormat.format(window.getEnd()));
                out.collect(next);

            }
        });

        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_uv_vc_ch_isnew_ar_10s_210927 values(?,?,?,?,?,?,?,?)"));

        env.execute();


    }
}
