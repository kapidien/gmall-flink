package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author shkstart
 * @create 2022-03-22 20:01
 */
public interface AsyncJoinFunction<T> {
     String getkey(T input);
     void join(T input, JSONObject dimInfo) throws ParseException, Exception;
}
