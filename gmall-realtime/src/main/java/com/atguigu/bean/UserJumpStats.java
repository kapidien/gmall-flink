package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shkstart
 * @create 2022-03-21 11:49
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserJumpStats {
    //统计开始时间
    private String stt;
    //统计结束时间
    private String edt;
    //维度：版本
    private String vc;
    //维度：渠道
    private String ch;
    //维度：地区
    private String ar;
    //维度：新老用户标识
    private String is_new;
    //度量： 跳出次数
    private Long uj_ct=0L;
    //统计时间
    private Long ts;
}
