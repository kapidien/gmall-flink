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
 * @create 2022-03-18 19:43
 */
public class OrderRefundWideApp {
    public static void main(String[] args) throws Exception {
        //create environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/210927");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(15));
        String topic  = "topic_db";
        String upsertTopic = "dwd_order_wide";
// TODO 从topic_db中读取业务数据
        tableEnv.executeSql("" +
                "CREATE TABLE ods_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING> " +
                ") " + MyKafkaUtils.getKafkaDDL(topic,"payment_wide_app_test"));
        //TODO 3.过滤出退单数据

        Table orderRefund = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['refund_type'] refund_type, " +
                "    data['refund_num'] refund_num, " +
                "    data['refund_amount'] refund_amount, " +
                "    data['refund_reason_type'] refund_reason_type, " +
                "    data['refund_reason_txt'] refund_reason_txt, " +
                "    data['refund_status'] refund_status, " +
                "    data['create_time'] create_time " +
                "from ods_db " +
                "where  `database` = 'gmall' " +
                "and `table` = 'order_refund_info' " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_refund_info",orderRefund);


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
                "    order_detail_coupon_create_time string " +
                ") " + MyKafkaUtils.getKafkaDDL(upsertTopic,"payment_wide_app_test"));

        //TODO 两表join
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    ori.id order_refund_id, " +
                "    ori.user_id, " +
                "    ori.order_id, " +
                "    ori.sku_id, " +
                "    ori.refund_type, " +
                "    ori.refund_num, " +
                "    ori.refund_amount, " +
                "    ori.refund_reason_type, " +
                "    ori.refund_reason_txt, " +
                "    ori.refund_status, " +
                "    ori.create_time order_refund_create_time, " +
                "    ow.order_detail_id, " +
                "    ow.sku_name, " +
                "    ow.img_url, " +
                "    ow.order_price, " +
                "    ow.sku_num, " +
                "    ow.create_time, " +
                "    ow.source_type, " +
                "    ow.source_id, " +
                "    ow.split_total_amount, " +
                "    ow.split_activity_amount, " +
                "    ow.split_coupon_amount, " +
                "    ow.consignee, " +
                "    ow.consignee_tel, " +
                "    ow.total_amount, " +
                "    ow.order_status, " +
                "    ow.payment_way, " +
                "    ow.delivery_address, " +
                "    ow.order_comment, " +
                "    ow.out_trade_no, " +
                "    ow.trade_body, " +
                "    ow.operate_time, " +
                "    ow.expire_time, " +
                "    ow.process_status, " +
                "    ow.tracking_no, " +
                "    ow.parent_order_id, " +
                "    ow.province_id, " +
                "    ow.activity_reduce_amount, " +
                "    ow.coupon_reduce_amount, " +
                "    ow.original_total_amount, " +
                "    ow.feight_fee, " +
                "    ow.feight_fee_reduce, " +
                "    ow.refundable_time, " +
                "    ow.order_detail_activity_id, " +
                "    ow.activity_id, " +
                "    ow.activity_rule_id, " +
                "    ow.order_detail_activity_create_time, " +
                "    ow.order_detail_coupon_id, " +
                "    ow.coupon_id, " +
                "    ow.coupon_use_id, " +
                "    ow.order_detail_coupon_create_time " +
                "from order_refund_info ori " +
                "join order_wide_result ow " +
                "on ori.order_id = ow.order_id " +
                "and ori.sku_id = ow.sku_id");

        tableEnv.createTemporaryView("result_table",resultTable);

        tableEnv.toRetractStream(resultTable, Row.class).print();


        //TODO 6.将数据写出

        tableEnv.executeSql("" +
                "create table order_refund_wide( " +
                "    order_refund_id string, " +
                "    user_id string, " +
                "    order_id string, " +
                "    sku_id string, " +
                "    refund_type string, " +
                "    refund_num string, " +
                "    refund_amount string, " +
                "    refund_reason_type string, " +
                "    refund_reason_txt string, " +
                "    refund_status string, " +
                "    order_refund_create_time string, " +
                "    order_detail_id string, " +
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
                "    PRIMARY KEY (`order_refund_id`) NOT ENFORCED " +
                ")" + MyKafkaUtils.getUpsertKafkaDDL("dwd_order_refund_wide"));

        tableEnv.executeSql("insert into order_refund_wide select * from result_table");

        //TODO 7.启动任务
        env.execute();
    }
}
