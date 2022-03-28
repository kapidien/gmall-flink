package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 19:14
 */
public interface VisitorStatsMapper {
    //新老访客流量统计
    @Select(" SELECT is_new, uj_ct, sv_ct, dur_sum, uv_ct" +
            " ,uj_ct" +
            " FROM (" +
            " SELECT is_new, sum(pv_ct) AS pv_ct, sum(sv_ct) AS sv_ct" +
            " ,sum(dur_sum) AS dur_sum" +
            " FROM dws_pv_vc_ch_isnew_ar_10s" +
            " WHERE toYYYYMMDD(stt) = #{date}" +
            " GROUP BY is_new" +
            " ) pv " +
            " JOIN (" +
            " SELECT is_new, sum(uv_ct) AS uv_ct" +
            " FROM dws_uv_vc_ch_isnew_ar_10s_210927" +
            " WHERE toYYYYMMDD(stt) = #{date}" +
            " GROUP BY is_new" +
            " ) uv " +
            " ON pv.is_new = uv.is_new" +
            " LEFT JOIN (" +
            " SELECT is_new, sum(uj_ct) AS uj_ct" +
            " FROM dws_userjump_vc_ch_isnew_ar_10s" +
            " WHERE toYYYYMMDD(stt) = #{date}" +
            " GROUP BY is_new" +
            " ) uj " +
            " ON pv.is_new = uj.is_new")
    public List<VisitorStats> selectMidStatsByNewFlag(int date);

    //分时流量统计
    @Select(" SELECT hr, new_uv, uj_ct, uv_ct, pv_ct " +
            " FROM (" +
            " SELECT toHour(stt) as hr, sum(pv_ct) AS pv_ct, sum(sv_ct) AS sv_ct," +
            " sum(dur_sum) AS dur_sum" +
            " FROM dws_pv_vc_ch_isnew_ar_10s" +
            " WHERE toYYYYMMDD(stt) = #{date}" +
            " GROUP BY toHour(stt)" +
            " ) pv " +
            " JOIN (" +
            " SELECT toHour(stt) as hr, sum(uv_ct) AS uv_ct,sum(if(is_new = '1',dws_uv_vc_ch_isnew_ar_10s_210927.uv_ct, 0)) AS new_uv " +
            " FROM dws_uv_vc_ch_isnew_ar_10s_210927" +
            " WHERE toYYYYMMDD(stt) = #{date}" +
            " GROUP BY toHour(stt)" +
            " ) uv " +
            "  ON pv.hr = uv.hr" +
            "  LEFT JOIN (" +
            " SELECT toHour(stt) as hr, sum(uj_ct) AS uj_ct" +
            " FROM dws_userjump_vc_ch_isnew_ar_10s" +
            " WHERE toYYYYMMDD(stt) = #{date}" +
            " GROUP BY toHour(stt)" +
            " ) uj" +
            "  ON pv.hr = uj.hr")
    public List<VisitorStats> selectMidStatsGroupbyHourNewFlag(int date);

    @Select("select count(pv.pv_ct) pv_ct from dws_pv_vc_ch_isnew_ar_10s pv where toYYYYMMDD(stt)=#{date}")
    public Long selectPv(int date);

    @Select("select count(uv.uv_ct) uv_ct from dws_uv_vc_ch_isnew_ar_10s_210927 uv where toYYYYMMDD(stt)=#{date}")
    public Long selectUv(int date);
}
