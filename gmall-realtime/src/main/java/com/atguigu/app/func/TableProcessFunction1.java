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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction1 extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction1(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"database":"","tableName":"","after":{"":"","":""...},"before":{"":"","":""...},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.校验表是否存在,如果不存在,则创建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkExtend());

        //3.将数据写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);

    }

    //校验表是否存在,如果不存在,则创建表
    //create table is not exists db.tn(id varchar primary key,name varchar,sex varchar) sinkExtend
    private void checkTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];

                //判断当前字段是否为主键
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }

                //判断是否为最后一个字段
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }

            //拼接扩展字段
            sql.append(")").append(sinkExtend);

            //打印SQL语句
            System.out.println(sql);

            //建表操作
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建表：" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
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

            //3.写出数据(过滤掉删除数据)
            if (!"delete".equals(value.getString("type"))) {
                value.put("sinkTable", tableProcess.getSinkTable());
                out.collect(value);
            }

        } else {
            System.out.println("Key:" + value.getString("table") + "不存在！");
        }
    }

    //data：{"id":"13","tm_name":"aaaa","logo_url":"bbb"}  sinkColumns："id,tn_name"
    private void filterColumns(JSONObject data, String sinkColumns) {

        String[] split = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(split);

        //        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        //        while (iterator.hasNext()) {
        //            Map.Entry<String, Object> next = iterator.next();
        //            if (!columnList.contains(next.getKey())) {
        //                iterator.remove();
        //            }
        //        }

        data.entrySet().removeIf(next -> !columnList.contains(next.getKey()));

    }

}
