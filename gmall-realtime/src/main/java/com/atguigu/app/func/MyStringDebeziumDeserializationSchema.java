package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-15 11:52
 */
public class MyStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {


    //Json格式

    /**
     * {
     * "database":"",
     * "tableName":"",
     * "after":{"":"","":""...},
     * "before":{"":"","":""...},
     * "type":""
     * }
     */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];

        //获取操作类型
        Struct value = (Struct)sourceRecord.value();
        //获取变化之后的数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null){
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object o = after.get(field);
                afterJson.put(field.name(),o);
            }
        }



        //获取变化之前的数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null){
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object o = before.get(field);
                beforeJson.put(field.name(),o);
            }
        }



        String type = value.getString("op");

        //将结果封装成jsonString

        JSONObject result = new JSONObject();
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("after",afterJson);
        result.put("before",beforeJson);
        result.put("type",type);

        collector.collect(result.toJSONString());



    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
