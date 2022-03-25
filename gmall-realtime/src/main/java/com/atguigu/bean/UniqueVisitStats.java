package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shkstart
 * @create 2022-03-19 14:50
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UniqueVisitStats {
    //统计开始时间
    private String stt;
    //统计结束时间
    private String edt;
    //维度：版本
    private String vc;
    //维度：渠道
    private String ch;

//    @TransientSink
////    private String bbb;

    //维度：地区
    private String ar;
    //维度：新老用户标识
    private String is_new;
    //度量：uv
    private Long uv_ct = 0L;
    //统计时间
    private Long ts;
}
