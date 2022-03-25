package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserProvinceSku;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author shkstart
 * @create 2022-03-22 19:56
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements AsyncJoinFunction<T>{
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;



    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    //查询维表信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getkey(input));
                    //将维表信息补充至数据上
                    join(input,dimInfo);

                    //将数据写出
                    resultFuture.complete(Collections.singleton(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }


            }


        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
