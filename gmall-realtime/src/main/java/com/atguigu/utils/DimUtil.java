package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-22 16:52
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection,String table,String key) throws Exception {
        //先查询redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        String jsonStr = jedis.get(redisKey);

        if (jsonStr != null){
            //重置过期时间
            jedis.expire(redisKey,24*60*60);
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }

        //拼接查询sqL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id = '" + key + "'";
        System.out.println(querySql);

        //查询
        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);


        //得到数据
        JSONObject dimInfo = jsonObjects.get(0);
        //将数据同步到redis
        jedis.set(redisKey,dimInfo.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();

        return dimInfo;


    }
    public static void deleteRedisDimInfo(String table,String key){
        //先查询redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        jedis.del(redisKey);
        jedis.close();
    }


    public static void main(String[] args) throws Exception {
//        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        System.out.println(getDimInfo(connection, "BASE_TRADEMARK", "19"));
//
//        long start = System.currentTimeMillis();
//
//        System.out.println(getDimInfo(connection, "BASE_TRADEMARK", "19"));
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
//        connection.close();
    }
}
