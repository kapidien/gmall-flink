package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 19:36
 */
public interface KeywordStatsMapper {

    @Select("select word,sum(ct) ct from dws_search_keyword_10s where toYYYYMMDD(stt)=#{date} group by word order by ct desc limit #{limit}")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);
}
