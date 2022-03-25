package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.UserProvinceSku;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author shkstart
 * @create 2022-03-24 14:54
 */
public class UserProvinceSku10sApp1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_order_wide";
        String groupId = "user_province_sku10s_app1";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtils.getKafkaStream2(topic, groupId));

        SingleOutputStreamOperator<JSONObject> map = streamSource.map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDS = map.keyBy(json -> json.getString("order_detail_id")).filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);

                valueStateDescriptor.enableTimeToLive(StateTtlConfig.
                        newBuilder(Time.seconds(5)).updateTtlOnReadAndWrite().build());

                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String state = valueState.value();

                if (state == null) {
                    valueState.update("0");
                    return true;
                } else {
                    return false;
                }
            }
        });
        SingleOutputStreamOperator<UserProvinceSku> mapDS = filterDS.map(json -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return UserProvinceSku.builder()
                    .sku_id(json.getLong("sku_id"))
                    .sku_name(json.getString("sku_name"))
                    .sku_price(json.getBigDecimal("order_price"))
                    .province_id(json.getLong("province_id"))
                    .order_ct(1L)
                    .order_sku_num(json.getLong("sku_num"))
                    .order_amount(json.getBigDecimal("split_total_amount"))
                    .ts(sdf.parse(json.getString("create_time")).getTime())
                    .build();
        });

        SingleOutputStreamOperator<UserProvinceSku> watermarks = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserProvinceSku>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserProvinceSku>() {
            @Override
            public long extractTimestamp(UserProvinceSku element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        KeyedStream<UserProvinceSku, Tuple3<Long, Long, Long>> keyedStream = watermarks.keyBy(new KeySelector<UserProvinceSku, Tuple3<Long, Long, Long>>() {
            @Override
            public Tuple3<Long, Long, Long> getKey(UserProvinceSku value) throws Exception {
                return Tuple3.of(value.getUser_id(), value.getProvince_id(), value.getSku_id());
            }
        });

        SingleOutputStreamOperator<UserProvinceSku> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<UserProvinceSku>() {
                    @Override
                    public UserProvinceSku reduce(UserProvinceSku value1, UserProvinceSku value2) throws Exception {
                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        value1.setOrder_ct(value1.getOrder_ct() + value2.getOrder_ct());
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                        return value1;
                    }
                }, new WindowFunction<UserProvinceSku, UserProvinceSku, Tuple3<Long, Long, Long>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<Long, Long, Long> longLongLongTuple3, TimeWindow window, Iterable<UserProvinceSku> input, Collector<UserProvinceSku> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        UserProvinceSku next = input.iterator().next();
                        next.setStt(sdf.format(window.getStart()));
                        next.setEdt(sdf.format(window.getEnd()));
                        out.collect(next);
                    }
                });

        SingleOutputStreamOperator<UserProvinceSku> userDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<UserProvinceSku>("DIM_USER_INFO") {
            @Override
            public String getkey(UserProvinceSku input) {
                return String.valueOf(input.getUser_id());
            }

            @Override
            public void join(UserProvinceSku input, JSONObject dimInfo) throws ParseException, Exception {
                String gender = dimInfo.getString("GENDER");
                String birthday = dimInfo.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                long age = (System.currentTimeMillis() - sdf.parse(birthday).getTime()) / 1000L / 24 / 60 / 60 / 24 / 365;
            }
        }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<UserProvinceSku> provinceNameDS = AsyncDataStream.unorderedWait(userDS
                , new DimAsyncFunction<UserProvinceSku>("DIM_BASE_PROVINCE") {

                    @Override
                    public String getkey(UserProvinceSku input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) {
                        input.setProvince_name(dimInfo.getString("NAME"));
                        input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        input.setProvince_iso_3166_2(dimInfo.getString("ISO_3166_2"));

                    }
                }, 60, TimeUnit.SECONDS);

//        provinceNameDS.print("province>>>");
        //7.3关联sku维表
        SingleOutputStreamOperator<UserProvinceSku> skuDS = AsyncDataStream.unorderedWait(provinceNameDS
                , new DimAsyncFunction<UserProvinceSku>("DIM_SKU_INFO") {
                    @Override
                    public String getkey(UserProvinceSku input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //关联spu维表
        SingleOutputStreamOperator<UserProvinceSku> spuDS = AsyncDataStream.unorderedWait(skuDS
                , new DimAsyncFunction<UserProvinceSku>("DIM_SPU_INFO") {
                    @Override
                    public String getkey(UserProvinceSku input) {
                        return String.valueOf(input.getSpu_id());
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) throws ParseException, Exception {
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);
        //关联category维表 3 2 1
        SingleOutputStreamOperator<UserProvinceSku> category3DS = AsyncDataStream.unorderedWait(spuDS
                , new DimAsyncFunction<UserProvinceSku>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getkey(UserProvinceSku input) {
                        return String.valueOf(input.getCategory3_id());
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) throws ParseException, Exception {
                        input.setCategory3_name(dimInfo.getString("NAME"));
                        input.setCategory2_id(dimInfo.getLong("CATEGORY2_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<UserProvinceSku> category2DS = AsyncDataStream.unorderedWait(category3DS
                , new DimAsyncFunction<UserProvinceSku>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getkey(UserProvinceSku input) {
                        return String.valueOf(input.getCategory2_id());
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) throws ParseException, Exception {
                        input.setCategory2_name(dimInfo.getString("NAME"));
                        input.setCategory1_id(dimInfo.getLong("CATEGORY1_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<UserProvinceSku> category1DS = AsyncDataStream.unorderedWait(category2DS
                , new DimAsyncFunction<UserProvinceSku>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getkey(UserProvinceSku input) {
                        return String.valueOf(input.getCategory1_id());
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) throws ParseException, Exception {
                        input.setCategory1_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);
        //关联trademark维表
        SingleOutputStreamOperator<UserProvinceSku> tmNameDS = AsyncDataStream.unorderedWait(category1DS
                , new DimAsyncFunction<UserProvinceSku>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getkey(UserProvinceSku input) {
                        return String.valueOf(input.getTm_id());
                    }

                    @Override
                    public void join(UserProvinceSku input, JSONObject dimInfo) throws ParseException, Exception {
                        input.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        tmNameDS.print("tmDS");
        //TODO 8.将数据写出
        tmNameDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_user_province_sku_10s values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));



    }
}
