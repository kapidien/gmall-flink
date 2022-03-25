package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shkstart
 * @create 2022-03-21 16:16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageViewStats {
    //统计开始时间
    private String stt;
    //统计结束时间
    private String edt;
    //维度：页面ID
    private String page_id;
    //维度：版本
    private String vc;
    //维度：渠道
    private String ch;
    //维度：地区
    private String ar;
    //维度：新老用户标识
    private String is_new;
    //度量： pv
    private Long pv_ct=0L;
    //度量： session ct
    private Long sv_ct=0L;
    //度量  页面停留时间毫秒
    private Long dur_time=0L;
    //统计时间
    private Long ts;
}
