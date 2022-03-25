package com.atguigu.app;

import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class OrderDetailApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境设置为Kafka分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5)); //给定的时间为公司最大数据延迟时间

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

        //TODO 2.使用DDL方式读取Kafka topic_db 主题的数据创建表
        String sourceTopic = "topic_db";
        String groupId = "order_detail_app_210927";
        String sinkTopic = "dwd_order_wide";
        tableEnv.executeSql("" +
                "create table ods_db( " +
                "    `database` String, " +
                "    `table` String, " +
                "    `type` String, " +
                "    `data` Map<String,String> " +
                ")" + MyKafkaUtils.getKafkaDDL(sourceTopic, groupId));

        //打印测试
        //        Table odsDbTable = tableEnv.sqlQuery("select * from ods_db");
        //        tableEnv.toAppendStream(odsDbTable, Row.class)
        //                .print(">>>>>>>>>>>");

        //TODO 3.过滤出订单明细表数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['img_url'] img_url, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount " +
                "from ods_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", orderDetailTable);

        //打印测试
        Table table = tableEnv.sqlQuery("select * from order_detail");
        tableEnv.toAppendStream(table, Row.class)
                .print(">>>>>>>>>>>");

//        //TODO 4.过滤出订单表
//        Table orderInfoTale = tableEnv.sqlQuery("" +
//                "select " +
//                "    data['id'] id, " +
//                "    data['consignee'] consignee, " +
//                "    data['consignee_tel'] consignee_tel, " +
//                "    data['total_amount'] total_amount, " +
//                "    data['order_status'] order_status, " +
//                "    data['user_id'] user_id, " +
//                "    data['payment_way'] payment_way, " +
//                "    data['delivery_address'] delivery_address, " +
//                "    data['order_comment'] order_comment, " +
//                "    data['out_trade_no'] out_trade_no, " +
//                "    data['trade_body'] trade_body, " +
//                "    data['create_time'] create_time, " +
//                "    data['operate_time'] operate_time, " +
//                "    data['expire_time'] expire_time, " +
//                "    data['process_status'] process_status, " +
//                "    data['tracking_no'] tracking_no, " +
//                "    data['parent_order_id'] parent_order_id, " +
//                "    data['img_url'] img_url, " +
//                "    data['province_id'] province_id, " +
//                "    data['activity_reduce_amount'] activity_reduce_amount, " +
//                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
//                "    data['original_total_amount'] original_total_amount, " +
//                "    data['feight_fee'] feight_fee, " +
//                "    data['feight_fee_reduce'] feight_fee_reduce, " +
//                "    data['refundable_time'] refundable_time " +
//                "from ods_db " +
//                "where `database` = 'gmall-210927-flink' " +
//                "and `table` = 'order_info' " +
//                "and `type` = 'insert'");
//
//        tableEnv.createTemporaryView("order_info", orderInfoTale);
//
//        //打印测试
////        Table table = tableEnv.sqlQuery("select * from order_info");
////        tableEnv.toAppendStream(table, Row.class)
////                .print(">>>>>>>>>>>");
//
//        //TODO 5.过滤出订单活动表
//        Table orderActivityTable = tableEnv.sqlQuery("" +
//                "select " +
//                "    data['id'] id, " +
//                "    data['order_id'] order_id, " +
//                "    data['order_detail_id'] order_detail_id, " +
//                "    data['activity_id'] activity_id, " +
//                "    data['activity_rule_id'] activity_rule_id, " +
//                "    data['sku_id'] sku_id, " +
//                "    data['create_time'] create_time " +
//                "from ods_db " +
//                "where `database` = 'gmall-210927-flink' " +
//                "and `table` = 'order_detail_activity' " +
//                "and `type` = 'insert'");
//
//        tableEnv.createTemporaryView("order_detail_activity", orderActivityTable);
//
//        //打印测试
////        Table table = tableEnv.sqlQuery("select * from order_detail_activity");
////        tableEnv.toAppendStream(table, Row.class)
////                .print(">>>>>>>>>>>");
//
//        //TODO 6.过滤出订单购物券表
//        Table orderDetailCouponTable = tableEnv.sqlQuery("" +
//                "select " +
//                "    data['id'] id, " +
//                "    data['order_id'] order_id, " +
//                "    data['order_detail_id'] order_detail_id, " +
//                "    data['coupon_id'] coupon_id, " +
//                "    data['coupon_use_id'] coupon_use_id, " +
//                "    data['sku_id'] sku_id, " +
//                "    data['create_time'] create_time " +
//                "from ods_db " +
//                "where `database` = 'gmall-210927-flink' " +
//                "and `table` = 'order_detail_coupon' " +
//                "and `type` = 'insert'");
//
//        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCouponTable);
//
//        //打印测试
////        Table table = tableEnv.sqlQuery("select * from order_detail_coupon");
////        tableEnv.toAppendStream(table, Row.class)
////                .print(">>>>>>>>>>>");
//
//        //TODO 7.关联4张表(left join)
//        Table orderWideTable = tableEnv.sqlQuery("" +
//                "select " +
//                "    od.id order_detail_id, " +
//                "    od.order_id, " +
//                "    od.sku_id, " +
//                "    od.sku_name, " +
//                "    od.order_price, " +
//                "    od.sku_num, " +
//                "    od.create_time, " +
//                "    od.source_type, " +
//                "    od.source_id, " +
//                "    od.split_total_amount, " +
//                "    od.split_activity_amount, " +
//                "    od.split_coupon_amount, " +
//                "    oi.consignee, " +
//                "    oi.consignee_tel, " +
//                "    oi.total_amount, " +
//                "    oi.order_status, " +
//                "    oi.user_id, " +
//                "    oi.payment_way, " +
//                "    oi.delivery_address, " +
//                "    oi.order_comment, " +
//                "    oi.out_trade_no, " +
//                "    oi.trade_body, " +
//                "    oi.operate_time, " +
//                "    oi.expire_time, " +
//                "    oi.process_status, " +
//                "    oi.tracking_no, " +
//                "    oi.parent_order_id, " +
//                "    oi.province_id, " +
//                "    oi.activity_reduce_amount, " +
//                "    oi.coupon_reduce_amount, " +
//                "    oi.original_total_amount, " +
//                "    oi.feight_fee, " +
//                "    oi.feight_fee_reduce, " +
//                "    oi.refundable_time, " +
//                "    oa.id order_activity_id, " +
//                "    oa.activity_id, " +
//                "    oa.activity_rule_id, " +
//                "    oa.create_time order_activity_create_time, " +
//                "    oc.id order_detail_coupon_id, " +
//                "    oc.coupon_id, " +
//                "    oc.coupon_use_id, " +
//                "    oc.create_time order_detail_coupon_create_time " +
//                "from order_detail od " +
//                "join order_info oi " +
//                "on od.order_id = oi.id " +
//                "left join order_detail_activity oa " +
//                "on od.id = oa.order_detail_id " +
//                "left join order_detail_coupon oc " +
//                "on od.id = oc.order_detail_id");
//
//        tableEnv.createTemporaryView("order_wide", orderWideTable);
//
//        //打印测试
////        Table table = tableEnv.sqlQuery("select * from order_wide");
////        tableEnv.toRetractStream(table, Row.class)
////                .print(">>>>>>>>>>");
//
//        //TODO 8.将数据写出到Kafka DWD层(upsert-kafka)
//        //8.1 创建Upsert-Kafka表
//        tableEnv.executeSql("" +
//                "create table order_wide_result( " +
//                "    order_detail_id String, " +
//                "    order_id String, " +
//                "    sku_id String, " +
//                "    sku_name String, " +
//                "    order_price String, " +
//                "    sku_num String, " +
//                "    create_time String, " +
//                "    source_type String, " +
//                "    source_id String, " +
//                "    split_total_amount String, " +
//                "    split_activity_amount String, " +
//                "    split_coupon_amount String, " +
//                "    consignee String, " +
//                "    consignee_tel String, " +
//                "    total_amount String, " +
//                "    order_status String, " +
//                "    user_id String, " +
//                "    payment_way String, " +
//                "    delivery_address String, " +
//                "    order_comment String, " +
//                "    out_trade_no String, " +
//                "    trade_body String, " +
//                "    operate_time String, " +
//                "    expire_time String, " +
//                "    process_status String, " +
//                "    tracking_no String, " +
//                "    parent_order_id String, " +
//                "    province_id String, " +
//                "    activity_reduce_amount String, " +
//                "    coupon_reduce_amount String, " +
//                "    original_total_amount String, " +
//                "    feight_fee String, " +
//                "    feight_fee_reduce String, " +
//                "    refundable_time String, " +
//                "    order_activity_id String, " +
//                "    activity_id String, " +
//                "    activity_rule_id String, " +
//                "    order_activity_create_time String, " +
//                "    order_detail_coupon_id String, " +
//                "    coupon_id String, " +
//                "    coupon_use_id String, " +
//                "    order_detail_coupon_create_time String, " +
//                "    PRIMARY KEY (order_detail_id) NOT ENFORCED " +
//                ")" + MyKafkaUtils.getUpsertKafkaDDL(sinkTopic));
//
//        //8.2 将数据写出
//        tableEnv.executeSql("insert into order_wide_result select * from order_wide")
//                .print();

        //TODO 9.启动任务
        env.execute("OrderDetailApp");

    }

}
