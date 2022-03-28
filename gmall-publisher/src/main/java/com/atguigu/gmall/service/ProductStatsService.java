package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 11:45
 */
public interface ProductStatsService {
    public BigDecimal getGMV(int date);

    //统计某天不同SPU商品交易额排名
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit);

    //统计某天不同类别商品交易额排名
    public List<ProductStats> getProductStatsGroupByCategory3(int date,int limit);

    //统计某天不同品牌商品交易额排名
    public List<ProductStats> getProductStatsByTrademark(int date,int limit);
}
