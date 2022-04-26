package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lqs
 * @Date 2022年04月23日 11:55:05
 * @Version 1.0.0
 * @ClassName TrafficHomeDetailPageViewBean
 * @Describe home和detail页面独立访问量实体表
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficHomeDetailPageView {

    /** 窗口起始时间 */
    String stt;
    /** 窗口结束时间 */
    String edt;
    /** 首页独立访客数 */
    Long homeUvCt;
    /** 商品详情页独立访客数 */
    Long goodDetailUvCt;
    /** 时间戳 */
    Long ts;

}
