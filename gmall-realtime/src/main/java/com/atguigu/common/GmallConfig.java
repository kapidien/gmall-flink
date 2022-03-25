package com.atguigu.common;

/**
 * @author shkstart
 * @create 2022-03-15 16:57
 */
public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL210927_REALTIME";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";


    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
