package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.ProvinceStats;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 18:28
 */
public interface ProvinceStatsService {
    public List<ProvinceStats> getProvinceStats(int date);
}
