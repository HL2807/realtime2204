package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月27日 16:07:19
 * @Version 1.0.0
 * @ClassName TradePaymentWindow
 * @Describe 支付成功事实表
 */

@Data
@AllArgsConstructor
public class TradePaymentWindow {

    /** 窗口起始时间 */
    String stt;

    /** 窗口终止时间 */
    String edt;

    /** 支付成功独立用户数 */
    Long paymentSucUniqueUserCount;

    /** 支付成功新用户数 */
    Long paymentSucNewUserCount;

    /** 时间戳 */
    Long ts;


}
