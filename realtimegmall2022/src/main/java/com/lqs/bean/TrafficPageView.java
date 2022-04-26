package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lqs
 * @Date 2022年04月22日 21:28:26
 * @Version 1.0.0
 * @ClassName TrafficPageView
 * @Describe 流量域页面实体表
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrafficPageView {

    /** 窗口起始时间*/
    String stt;
    /** 窗口结束时间*/
    String edt;
    /** app 版本号*/
    String vc;
    /** 渠道*/
    String ch;
    /** 地区*/
    String ar;
    /** 新老访客状态标记*/
    String isNew;
    /** 独立访客数*/
    Long uvCt;
    /** 会话数*/
    Long svCt;
    /** 页面浏览数*/
    Long pvCt;
    /** 累计访问时长*/
    Long durSum;
    /** 跳出会话数*/
    Long ujCt;
    /** 时间戳*/
    Long ts;

}
