package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @author shkstart
 * @create 2022-03-18 14:01
 */
//数据流：web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序： Mock -> Mysql -> Maxwell -> Kafka(ZK) -> OrderDetailApp -> Kafka(ZK)
public class OrderDetailApp {
    public static void main(String[] args) throws Exception {
      //create environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/210927");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5)); //给定的时间为公司最大数据延迟时间

        //从topic_db中读取业务数据
        String topic  = "topic_db";
        String groupId = "210927";
        String upsertTopic = "dwd_order_wide";

        tableEnv.executeSql("" +
                "CREATE TABLE ods_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING> " +
                ") " + MyKafkaUtils.getKafkaDDL(topic,groupId));

        //TODO 3.过滤出订单明细表数据

        Table orderDetail = tableEnv.sqlQuery("" +
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
        tableEnv.createTemporaryView("order_detail",orderDetail);

//        orderDetail.execute().print();

//                Table table = tableEnv.sqlQuery("select * from order_detail");
//        tableEnv.toAppendStream(table, Row.class)
//                .print(">>>>>>>>>>>");


        //TODO 4.过滤出订单表

        Table orderInfo = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['consignee'] consignee, " +
                "    data['consignee_tel'] consignee_tel, " +
                "    data['total_amount'] total_amount, " +
                "    data['order_status'] order_status, " +
                "    data['user_id'] user_id, " +
                "    data['payment_way'] payment_way, " +
                "    data['delivery_address'] delivery_address, " +
                "    data['order_comment'] order_comment, " +
                "    data['out_trade_no'] out_trade_no, " +
                "    data['trade_body'] trade_body, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['expire_time'] expire_time, " +
                "    data['process_status'] process_status, " +
                "    data['tracking_no'] tracking_no, " +
                "    data['parent_order_id'] parent_order_id, " +
                "    data['img_url'] img_url, " +
                "    data['province_id'] province_id, " +
                "    data['activity_reduce_amount'] activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    data['original_total_amount'] original_total_amount, " +
                "    data['feight_fee'] feight_fee, " +
                "    data['feight_fee_reduce'] feight_fee_reduce, " +
                "    data['refundable_time'] refundable_time " +
                "from ods_db " +
                "where  `database` = 'gmall' " +
                "and `table` = 'order_info' " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_info",orderInfo);

//        orderInfo.execute().print();


//                Table table = tableEnv.sqlQuery("select * from order_info");
//        tableEnv.toAppendStream(table, Row.class)
//                .print(">>>>>>>>>>>");

        //TODO 5.过滤出订单活动表
        Table orderActivity = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['activity_id'] activity_id, " +
                "    data['activity_rule_id'] activity_rule_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from ods_db " +
                "where  `database` = 'gmall' " +
                "and `table` = 'order_detail_activity' " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_activity",orderActivity);

//        tableEnv.toAppendStream(orderActivity,Row.class).print();


        //TODO 6.过滤出订单购物券表

        Table orderCoupon = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['coupon_id'] coupon_id, " +
                "    data['coupon_use_id'] coupon_use_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from ods_db " +
                "where  `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_coupon",orderCoupon);

//        tableEnv.toAppendStream(orderCoupon,Row.class).print();

        //TODO 7.关联4张表(left join)

        Table orderWideTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id order_detail_id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.img_url, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.payment_way, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.operate_time, " +
                "    oi.expire_time, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oa.id order_detail_activity_id, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oa.create_time order_detail_activity_create_time, " +
                "    oc.id order_detail_coupon_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id, " +
                "    oc.create_time order_detail_coupon_create_time " +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id = oi.id " +
                "left join order_detail_activity oa " +
                "on od.id = oa.order_detail_id " +
                "left join order_detail_coupon oc " +
                "on od.id = oc.order_detail_id");

        tableEnv.createTemporaryView("order_wide_table",orderWideTable);
        tableEnv.toRetractStream(orderWideTable,Row.class).print();

        //TODO 8.将数据写出到Kafka DWD层(upsert-kafka)
        tableEnv.executeSql("" +
                "create table order_wide_result( " +
                "    order_detail_id string, " +
                "    order_id string, " +
                "    sku_id string, " +
                "    sku_name string, " +
                "    img_url string, " +
                "    order_price string, " +
                "    sku_num string, " +
                "    create_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    split_total_amount string, " +
                "    split_activity_amount string, " +
                "    split_coupon_amount string, " +
                "    consignee string, " +
                "    consignee_tel string, " +
                "    total_amount string, " +
                "    order_status string, " +
                "    user_id string, " +
                "    payment_way string, " +
                "    delivery_address string, " +
                "    order_comment string, " +
                "    out_trade_no string, " +
                "    trade_body string, " +
                "    operate_time string, " +
                "    expire_time string, " +
                "    process_status string, " +
                "    tracking_no string, " +
                "    parent_order_id string, " +
                "    province_id string, " +
                "    activity_reduce_amount string, " +
                "    coupon_reduce_amount string, " +
                "    original_total_amount string, " +
                "    feight_fee string, " +
                "    feight_fee_reduce string, " +
                "    refundable_time string, " +
                "    order_detail_activity_id string, " +
                "    activity_id string, " +
                "    activity_rule_id string, " +
                "    order_detail_activity_create_time string, " +
                "    order_detail_coupon_id string, " +
                "    coupon_id string, " +
                "    coupon_use_id string, " +
                "    order_detail_coupon_create_time string, " +
                "    PRIMARY KEY (`order_detail_id`) NOT ENFORCED " +
                ") "
                + MyKafkaUtils.getUpsertKafkaDDL(upsertTopic));

        tableEnv.executeSql("insert into order_wide_result select * from order_wide_table");





        //TODO 9.启动任务
        env.execute("OrderDetailApp");






    }
}
