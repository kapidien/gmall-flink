package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author shkstart
 * @create 2022-03-19 16:56
 */
public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getClickHouseSink(String sql){

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
           @SneakyThrows
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                Class<?> clazz = t.getClass();

                Field[] fields = clazz.getDeclaredFields();
                int offset = 0;
                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];

                    field.setAccessible(true);
                    //todo 获取字段上的不写出到ck的注解
                    TransientSink annotation = field.getAnnotation(TransientSink.class);
                    if (annotation != null){
                        offset++;
                        continue;
                    }

                    Object value = field.get(t);

                    preparedStatement.setObject(i+1-offset,value);
                }

            }
        }, JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());



    }
}
