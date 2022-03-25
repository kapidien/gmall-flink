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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author shkstart
 * @create 2022-03-19 14:16
 */
public class UniqueVisit10sApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStateBackend(new HashMapStateBackend());
        env.setParallelism(1);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/210927");


        String topic = "dwd_page_log";
        String groupId  = "UniqueVisit10sApp";

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtils.getKafkaStream(topic, groupId));

        //将数据解析成json
        SingleOutputStreamOperator<JSONObject> map = streamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });

        //分配watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = map.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, Object> keyedStream = jsonObjWithWMDS.keyBy(json -> {
            return json.getJSONObject("common").getString("mid");
        });

        //根据mid进行去重过滤

        SingleOutputStreamOperator<JSONObject> filter = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastTimeState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> timeState = new ValueStateDescriptor<>("time_state", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                timeState.enableTimeToLive(stateTtlConfig);

                lastTimeState = getRuntimeContext().getState(timeState);


                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");


                if (lastPageId == null) {
                    String lastTime = lastTimeState.value();
                    String currentTime = sdf.format(value.getLong("ts"));

                    if (currentTime.equals(lastTime)) {
                        return false;
                    }
                    lastTimeState.update(currentTime);
                    return true;
                }

                return false;
            }
        });

        filter.print("filter>>>");

        SingleOutputStreamOperator<UniqueVisitStats> uniqueVisitStatsDS = filter.map(json -> {
            JSONObject common = json.getJSONObject("common");
            return new UniqueVisitStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    json.getLong("ts")
            );
        });

        KeyedStream<UniqueVisitStats, Tuple4<String, String, String, String>> uniqueVisitStatsTuple4KeyedStream = uniqueVisitStatsDS.keyBy(new KeySelector<UniqueVisitStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(UniqueVisitStats value) throws Exception {
                return Tuple4.of(value.getAr(), value.getCh(), value.getVc(), value.getIs_new());
            }
        });

        WindowedStream<UniqueVisitStats, Tuple4<String, String, String, String>, TimeWindow> window = uniqueVisitStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));


        SingleOutputStreamOperator<UniqueVisitStats> reduce = window.reduce(new ReduceFunction<UniqueVisitStats>() {
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


        reduce.print("result>>");

        //todo 将数据写入ck
        reduce.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_uv_vc_ch_isnew_ar_10s_210927 values(?,?,?,?,?,?,?,?)"));




        env.execute();

    }
}
