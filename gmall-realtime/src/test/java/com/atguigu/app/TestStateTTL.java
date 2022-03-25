package com.atguigu.app;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestStateTTL {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        dataStreamSource.map(new RichMapFunction<String, Object>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("state", String.class);

                //给状态设置TTL
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(stateTtlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Object map(String value) throws Exception {
                return null;
            }
        });

    }

}
