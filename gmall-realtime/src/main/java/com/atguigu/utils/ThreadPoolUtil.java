package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author shkstart
 * @create 2022-03-22 18:59
 */
public class ThreadPoolUtil {
    //声明线程池
    public static ThreadPoolExecutor pool;

    private ThreadPoolUtil(){

    }
    //单例对象
    public static ThreadPoolExecutor getInstance(){

        if (pool== null){
            synchronized (ThreadPoolUtil.class){
                if (pool == null){
                    pool = new ThreadPoolExecutor(4,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}
