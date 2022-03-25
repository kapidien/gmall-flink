package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @author shkstart
 * @create 2022-03-18 18:54
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //create environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/210927");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(15));
        String topic  = "topic_db";
        String groupId = "payment_wide_app_test";
        String upsertTopic = "dwd_order_wide";
// TODO 从topic_db中读取业务数据
        tableEnv.executeSql("" +
                "CREATE TABLE ods_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING> " +
                ") " + MyKafkaUtils.getKafkaDDL(topic,groupId));
       //TODO 3.过滤出支付数据

        Table paymentInfo = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id,  " +
                "    data['out_trade_no'] out_trade_no,  " +
                "    data['order_id'] order_id,  " +
                "    data['user_id'] user_id,  " +
                "    data['payment_type'] payment_type,  " +
                "    data['trade_no'] trade_no,  " +
                "    data['total_amount'] total_amount,  " +
                "    data['subject'] subject,  " +
                "    data['payment_status'] payment_status,  " +
                "    data['create_time'] create_time,  " +
                "    data['callback_time'] callback_time,  " +
                "    data['callback_content'] callback_content  " +
                "from ods_db " +
                "where  `database` = 'gmall' " +
                "and `table` = 'payment_info' " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("payment_info",paymentInfo);

        //TODO 获取dwd_order_wide 订单明细宽表的数据

        tableEnv.executeSql("" +
                "create table order_wide_result(  " +
                "    order_detail_id String,  " +
                "    order_id String,  " +
                "    sku_id String,  " +
                "    sku_name String,  " +
                "    order_price String,  " +
                "    sku_num String,  " +
                "    create_time String,  " +
                "    source_type String,  " +
                "    source_id String,  " +
                "    split_total_amount String,  " +
                "    split_activity_amount String,  " +
                "    split_coupon_amount String,  " +
                "    consignee String,  " +
                "    consignee_tel String,  " +
                "    total_amount String,  " +
                "    order_status String,  " +
                "    user_id String,  " +
                "    payment_way String,  " +
                "    delivery_address String,  " +
                "    order_comment String,  " +
                "    trade_body String,  " +
                "    operate_time String,  " +
                "    expire_time String,  " +
                "    process_status String,  " +
                "    tracking_no String,  " +
                "    parent_order_id String,  " +
                "    province_id String,  " +
                "    activity_reduce_amount String,  " +
                "    coupon_reduce_amount String,  " +
                "    original_total_amount String,  " +
                "    feight_fee String,  " +
                "    feight_fee_reduce String,  " +
                "    refundable_time String,  " +
                "    order_activity_id String,  " +
                "    activity_id String,  " +
                "    activity_rule_id String,  " +
                "    order_activity_create_time String,  " +
                "    order_detail_coupon_id String,  " +
                "    coupon_id String,  " +
                "    coupon_use_id String,  " +
                "    order_detail_coupon_create_time String " +
                ")" + MyKafkaUtils.getKafkaDDL(upsertTopic,"payment_wide_app_test"));

        //TODO join两表

        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "pi.id payment_id, " +
                "pi.out_trade_no, " +
                "pi.subject, " +
                "pi.payment_status, " +
                "pi.create_time payment_create_time, " +
                "pi.callback_time, " +
                "pi.callback_content, " +
                "ow.order_detail_id, " +
                "ow.order_id, " +
                "ow.sku_id, " +
                "ow.sku_name, " +
                "ow.order_price, " +
                "ow.sku_num, " +
                "ow.create_time, " +
                "ow.source_type, " +
                "ow.source_id, " +
                "ow.split_total_amount, " +
                "ow.split_activity_amount, " +
                "ow.split_coupon_amount, " +
                "ow.consignee, " +
                "ow.consignee_tel, " +
                "ow.total_amount, " +
                "ow.order_status, " +
                "ow.user_id, " +
                "ow.payment_way, " +
                "ow.delivery_address, " +
                "ow.order_comment, " +
                "ow.trade_body, " +
                "ow.operate_time, " +
                "ow.expire_time, " +
                "ow.process_status, " +
                "ow.tracking_no, " +
                "ow.parent_order_id, " +
                "ow.province_id, " +
                "ow.activity_reduce_amount, " +
                "ow.coupon_reduce_amount, " +
                "ow.original_total_amount, " +
                "ow.feight_fee, " +
                "ow.feight_fee_reduce, " +
                "ow.refundable_time, " +
                "ow.order_activity_id, " +
                "ow.activity_id, " +
                "ow.activity_rule_id, " +
                "ow.order_activity_create_time, " +
                "ow.order_detail_coupon_id, " +
                "ow.coupon_id, " +
                "ow.coupon_use_id, " +
                "ow.order_detail_coupon_create_time " +
                "from payment_info pi " +
                "left join order_wide_result ow " +
                "on pi.order_id = ow.order_id");
//        tableEnv.toRetractStream(resultTable, Row.class).print();

        tableEnv.createTemporaryView("result_table",resultTable);

        tableEnv.executeSql("" +
                "create table payment_wide( " +
                "payment_id string, " +
                "out_trade_no string, " +
                "subject string, " +
                "payment_status string, " +
                "payment_create_time string, " +
                "callback_time string, " +
                "callback_content string, " +
                "order_detail_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "order_price string, " +
                "sku_num string, " +
                "create_time string, " +
                "source_type string, " +
                "source_id string, " +
                "split_total_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "consignee string, " +
                "consignee_tel string, " +
                "total_amount string, " +
                "order_status string, " +
                "user_id string, " +
                "payment_way string, " +
                "delivery_address string, " +
                "order_comment string, " +
                "trade_body string, " +
                "operate_time string, " +
                "expire_time string, " +
                "process_status string, " +
                "tracking_no string, " +
                "parent_order_id string, " +
                "province_id string, " +
                "activity_reduce_amount string, " +
                "coupon_reduce_amount string, " +
                "original_total_amount string, " +
                "feight_fee string, " +
                "feight_fee_reduce string, " +
                "refundable_time string, " +
                "order_activity_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "order_activity_create_time string, " +
                "order_detail_coupon_id string, " +
                "coupon_id string, " +
                "coupon_use_id string, " +
                "order_detail_coupon_create_time string, " +
                "PRIMARY KEY (`payment_id`) NOT ENFORCED " +
                " " +
                ")" + MyKafkaUtils.getUpsertKafkaDDL("dwd_payment_wide"));

        tableEnv.executeSql("insert into payment_wide select * from result_table").print();

        env.execute();


    }
}
