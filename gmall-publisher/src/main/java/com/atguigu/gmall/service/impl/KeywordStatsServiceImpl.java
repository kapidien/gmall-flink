package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.KeywordStats;
import com.atguigu.gmall.mapper.KeywordStatsMapper;
import com.atguigu.gmall.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 19:38
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
   @Autowired
   KeywordStatsMapper keywordStatsMapper;


    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
