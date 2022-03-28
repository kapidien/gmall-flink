package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.mapper.ProvinceStatsMapper;
import com.atguigu.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 18:30
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;
    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
