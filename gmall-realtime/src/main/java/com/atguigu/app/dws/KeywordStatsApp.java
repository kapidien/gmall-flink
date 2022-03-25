package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author shkstart
 * @create 2022-03-23 18:15
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境设置为Kafka分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //        //1.1 开启CheckPoint
        //        env.enableCheckpointing(5 * 60000L);
        //        env.getCheckpointConfig().setCheckpointTimeout(5 * 60000L);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //
        //        //1.2 指定状态后端
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/210927/flink-state");

        //TODO 2.使用DDL的方式读取Kafka dwd_page_log主题的数据创建动态表,同时提取时间戳生成Watermark
        tableEnv.executeSql("create table page_log( " +
                "    `page` Map<String,String>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                "    )" + MyKafkaUtils.getKafkaDDL("dwd_page_log","keyword_stats_app"));
        //TODO 3.过滤出搜索数据
        Table keyWordTable = tableEnv.sqlQuery("select " +
                "    page['item'] key_word, " +
                "    rt " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("key_word_table",keyWordTable);

        //TODO 4.注册UDTF,切词
        tableEnv.createTemporaryFunction("SplitFunction",SplitFunction.class);
        Table wordTable = tableEnv.sqlQuery("" +
                "select" +
                "    word, " +
                "    rt " +
                "from key_word_table, " +
                "LATERAL TABLE(SplitFunction(key_word))");
        tableEnv.createTemporaryView("word_table",wordTable);


        //TODO 5.词频统计,分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss')  stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss')  edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from word_table " +
                "group by word, " +
                "TUMBLE(rt,INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流.
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 7.将数据写出到ClickHouse
        keywordStatsDataStream.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_search_keyword_10s(word,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute();

    }
}
