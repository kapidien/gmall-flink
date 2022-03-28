package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.KeywordStats;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 19:37
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
