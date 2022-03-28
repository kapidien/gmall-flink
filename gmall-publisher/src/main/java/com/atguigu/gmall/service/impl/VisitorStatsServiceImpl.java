package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.VisitorStats;
import com.atguigu.gmall.mapper.VisitorStatsMapper;
import com.atguigu.gmall.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 19:17
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectMidStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHr(int date) {
        return visitorStatsMapper.selectMidStatsGroupbyHourNewFlag(date);
    }
}
