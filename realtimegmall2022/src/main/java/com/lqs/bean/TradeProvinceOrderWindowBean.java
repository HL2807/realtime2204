package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * @Author lqs
 * @Date 2022年04月27日 11:43:22
 * @Version 1.0.0
 * @ClassName TradeProvinceOrderWindow
 * @Describe 交易域省份粒度下单各窗口汇总表实体类
 */

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindowBean {

    /** 窗口起始时间 */
    String stt;
    /** 窗口结束时间 */
    String edt;
    /** 省份 ID */
    String provinceId;
    /** 省份名称 */
    @Builder.Default
    String provinceName = "";
    /** 累计下单次数 */
    Long orderCount;
    /** 累计下单金额 */
    Double orderAmount;
    /** 时间戳 */
    Long ts;

}
