package com.atguigu.app.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @create 2022-03-15 16:51
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"database":"","tableName":"","after":{"":"","":""...},"before":{"":"","":""...},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.获取并解析广播流的数据
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2. 检验表是否存在
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkExtend());

        //3.将数据写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);



    }
    private void checkTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend){
        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }

            if (sinkExtend == null){
                sinkExtend = "";
            }
            StringBuffer sql = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断当前字段是否为主键
                if (sinkPk.equals(column)){
                    sql.append(column).append(" varchar primary key");
                }else {
                    sql.append(column).append(" varchar");
                }
                if (i < columns.length - 1){
                    sql.append(",");
                }

            }
            //拼接字段
            sql.append(")").append(sinkExtend);

            //打印sql语句
            System.out.println(sql);

            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("创建表： " + sinkTable + "失败");
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
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));
        if (tableProcess != null) {
            //2.过滤字段
            filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());
            //3.写出数据;
            if (!"delete".equals(value.getString("type"))) {
                value.put("sinkTable", tableProcess.getSinkTable());
                out.collect(value);
            }
        } else {
            System.out.println("Key:" + value.getString("table") + "不存在");
        }



    }
    //data：{"id":"13","tm_name":"aaaa","logo_url":"bbb"}  sinkColumns："id,tn_name"
    private void filterColumns(JSONObject data, String sinkColumns){
        String[] split = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(split);
        //        while (iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())){
//                iterator.remove();
//            }
//
//        }
        data.entrySet().removeIf(next -> !columnList.contains(next.getKey()));


    }


}
