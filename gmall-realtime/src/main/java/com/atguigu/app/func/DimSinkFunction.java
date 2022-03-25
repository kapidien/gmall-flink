package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @author shkstart
 * @create 2022-03-24 15:42
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取类型
        String type = value.getString("type");
        if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)){
            return;
        }
        //拼接sql
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        String sql = gensql(sinkTable,data);
        System.out.println(sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        if ("update".equals(type)){
            DimUtil.deleteRedisDimInfo(sinkTable.toUpperCase(),data.getString("id"));
        }
        preparedStatement.execute();
        connection.commit();
        preparedStatement.close();

    }

    private String gensql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();

        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable +
                "(" + StringUtils.join(keySet,",") + ") values('"  +
                StringUtils.join(values,"','") + "')";



    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
