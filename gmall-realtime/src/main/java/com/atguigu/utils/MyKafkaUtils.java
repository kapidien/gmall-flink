package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author shkstart
 * @create 2022-03-12 21:13
 */
public class MyKafkaUtils {
    private static  Properties props = new Properties();
    static {
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
    }
    public static FlinkKafkaConsumer<String> getKafkaStream(String topic,String groupId){

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

    public static FlinkKafkaConsumer<String> getKafkaStream2(String topic,String groupId){

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,
                new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                byte[] value = record.value();
                if (value == null){
                    return "";
                }else {
                    return new String(value);
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }, props);
    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){

        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),props);
    }

    public static FlinkKafkaProducer<String> getKafkaProducer2(String topic){

        return new FlinkKafkaProducer<String>("default_topic", new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(topic,element.getBytes());
            }
        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
    public static String getKafkaDDL(String topic, String groupId){
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092', " +
                "  'properties.group.id' = '" + groupId + "', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'json' " +
                ")";
    }
    public static String getUpsertKafkaDDL(String topic){
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
