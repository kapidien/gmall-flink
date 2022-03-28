package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 18:25
 */
public interface ProvinceStatsMapper {
    //按地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount from dws_user_province_sku_10s where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    public List<ProvinceStats> selectProvinceStats(int date);
}
