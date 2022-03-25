package com.atguigu.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author shkstart
 * @create 2022-03-22 14:00
 */
@Data
@Builder
public class UserProvinceSku {

    String stt;  //窗口起始时间
    String edt;  //窗口结束时间

    Long sku_id;           //sku编号
    String sku_name;       //sku名称
    BigDecimal sku_price;  //sku单价

    Long spu_id; //spu编号
    String spu_name;//spu名称
    Long tm_id; //品牌编号
    String tm_name;//品牌名称
    Long category3_id;//品类编号  3级
    String category3_name;//品类名称
    Long category2_id;//品类编号 2级
    String category2_name;//品类名称
    Long category1_id;//品类编号  1级
    String category1_name;//品类名称

    Long user_id;
    Long user_age;
    String user_gender;

    Long province_id; //地区编码
    String province_name;
    String province_area_code;
    String province_iso_code;
    String province_iso_3166_2;

    Long order_sku_num = 0L; //下单商品个数
    BigDecimal order_amount = BigDecimal.ZERO;//下单商品金额
    Long order_ct = 0L; //订单数

    Long ts; //统计时间戳
}