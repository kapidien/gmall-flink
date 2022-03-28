package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 11:43
 */
public interface ProductStatsMapper {

    //获取交易额
    @Select("select sum(order_amount) order_amount from dws_user_province_sku_10s where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getGMV(int date);

    @Select("select spu_id,spu_name,sum( order_amount) order_amount,sum(dws_user_province_sku_10s.order_ct) order_ct from dws_user_province_sku_10s where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name having order_amount > 0 order by order_amount  desc limit #{limit} ")
    public List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);


    @Select("select tm_id,tm_name,sum(order_amount) order_amount from dws_user_province_sku_10s where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name having order_amount>0 order by order_amount  desc limit #{limit}")
    public List<ProductStats> getProductStatsByTrademark(@Param("date") int date, @Param("limit") int limit);


    @Select("select category3_id,category3_name,sum(order_amount) order_amount from dws_user_province_sku_10s where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name having order_amount>0 order by order_amount desc limit #{limit}")
    public List<ProductStats> getProductStatsGroupByCategory3(@Param("date") int date, @Param("limit") int limit);
}
