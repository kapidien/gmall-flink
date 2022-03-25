package com.atguigu.bean;

import lombok.Data;

/**
 * @author shkstart
 * @create 2022-03-15 16:45
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
