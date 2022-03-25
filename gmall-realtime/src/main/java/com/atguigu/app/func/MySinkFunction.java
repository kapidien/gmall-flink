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
 * @create 2022-03-16 10:23
 */
public class MySinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    //{"database":"gmall","table":"base_trademark","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,
// "data":{"id":"13","tm_name":"aaaa","logo_url":"bbb"},"old":null}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        //获取类型
        String type = value.getString("type");
        if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
            return;
        }

        //拼接SQL语句:upsert into db.tn(id,name,sex) values('..','..','..')
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        String sql = gensql(sinkTable, data);

        //打印sql语句
        System.out.println(sql);
        //编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        if ("update".equals(value.getString("type"))){
            DimUtil.deleteRedisDimInfo(sinkTable.toUpperCase(),data.getString("id"));
        }
        preparedStatement.execute();


        connection.commit();

        preparedStatement.close();


    }
    //    upsert into t(id,name,sex) values('...','...','...')
    private String gensql(String sinkTable, JSONObject data) {
        Set<String> strings = data.keySet();
        Collection<Object> values = data.values();


        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + StringUtils.join(strings,",") + ") values('" + StringUtils.join(values,"','")
                 + "')";
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
