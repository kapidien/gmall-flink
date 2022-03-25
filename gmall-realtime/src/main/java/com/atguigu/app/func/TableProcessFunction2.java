package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author shkstart
 * @create 2022-03-17 14:06
 */
public class TableProcessFunction2 extends BroadcastProcessFunction<JSONObject, String, JSONObject> {


    private Connection connection;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction2(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);
       //判断表是否存在不存在则创建
        isExistOrCreate(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());
        //将数据写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);


    }

    private void isExistOrCreate(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if(sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }
            if (sinkExtend == null ){
                sinkExtend = "";
            }
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] split = sinkColumns.split(",");

            for (int i = 0; i < split.length; i++) {
               if (sinkPk.equals(split[i])){
                   sql.append(split[i])
                           .append(" varchar primary key");
               }else {
                   sql.append(split[i])
                           .append(" varchar");
               }

               if (i < split.length-1){
                   sql.append(",");
               }
            }
            sql.append(")").append(sinkExtend);
            System.out.println(sql);
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                preparedStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }


    }

    //value:{"database":"gmall","table":"base_trademark","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":"13","tm_name":"aaaa","logo_url":"bbb"},"old":null}

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));

        if (tableProcess != null ){
            //过滤数据
            filterColumns(tableProcess.getSinkColumns(),value.getJSONObject("data"));
            if (!"delete".equals(value.getString("type"))){
                value.put("sinkTable",tableProcess.getSinkTable());
                out.collect(value);
            }
        }else {
            System.out.println("key:" + value.getString("table") + "不存在");
        }


    }

    private void filterColumns(String sinkColumns, JSONObject data) {
        String[] split = sinkColumns.split(",");
        List<String> strings = Arrays.asList(split);

        data.entrySet().removeIf(next -> !strings.contains(next.getKey()));
    }


}
