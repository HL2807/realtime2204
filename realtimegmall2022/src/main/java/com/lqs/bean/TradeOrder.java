package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月24日 15:18:33
 * @Version 1.0.0
 * @ClassName TradeOrder
 * @Describe 用户下单求和事实表
 */
@Data
@AllArgsConstructor
public class TradeOrder {

    /** 窗口起始时间 */
    String stt;
    /** 窗口关闭时间 */
    String edt;
    /** 下单独立用户数 */
    Long orderUniqueUserCount;
    /** 下单新用户数 */
    Long orderNewUserCount;
    /** 下单活动减免金额 */
    Double orderActivityReduceAmount;
    /** 下单优惠券减免金额 */
    Double orderCouponReduceAmount;
    /** 下单原始金额 */
    Double orderOriginalTotalAmount;
    /** 时间戳 */
    Long ts;

}
