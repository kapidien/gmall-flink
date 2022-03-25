package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.PageViewStats;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.java2d.pipe.ValidatePipe;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author shkstart
 * @create 2022-03-21 16:14
 */
public class PageViewStats10sApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_page_log";
        String groupId  = "pageViewStats10sApp";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtils.getKafkaStream(topic, groupId));

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<PageViewStats> pageViewStatsDS = streamSource.map(new MapFunction<String, PageViewStats>() {
            @Override
            public PageViewStats map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");

                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                return new PageViewStats("", "",
                        jsonObject.getJSONObject("page").getString("page_id"),
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L,
                        lastPageId == null ? 0L : 1L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts"));
            }
        });


        //TODO 4.提取事件时间生成WaterMark，分组、开窗、聚合
        WindowedStream<PageViewStats, Tuple5<String, String, String, String, String>, TimeWindow> window = pageViewStatsDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PageViewStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<PageViewStats>() {
            @Override
            public long extractTimestamp(PageViewStats element, long recordTimestamp) {
                return element.getTs();
            }
        })).keyBy(new KeySelector<PageViewStats, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> getKey(PageViewStats value) throws Exception {
                return Tuple5.of(value.getPage_id(), value.getAr(), value.getCh(), value.getVc(), value.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<PageViewStats> reduce = window.reduce(new ReduceFunction<PageViewStats>() {
            @Override
            public PageViewStats reduce(PageViewStats value1, PageViewStats value2) throws Exception {
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setDur_time(value1.getDur_time() + value2.getDur_time());
                return value1;
            }
        }, new WindowFunction<PageViewStats, PageViewStats, Tuple5<String, String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple5<String, String, String, String, String> stringStringStringStringStringTuple5, TimeWindow window, Iterable<PageViewStats> input, Collector<PageViewStats> out) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                PageViewStats next = input.iterator().next();
                next.setStt(simpleDateFormat.format(window.getStart()));
                next.setEdt(simpleDateFormat.format(window.getEnd()));
                out.collect(next);
            }
        });


        reduce.print("result>>>");

        reduce.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_pv_vc_ch_isnew_ar_10s values(?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();

    }
}
