package com.lqs.bean;

import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月21日 21:32:59
 * @Version 1.0.0
 * @ClassName CouponUsePay
 * @Describe 工具域优惠券支付事实表
 */
@Data
public class CouponUsePay {

    // 优惠券领用记录 id
    String id;

    // 优惠券 id
    String coupon_id;

    // 用户 id
    String user_id;

    // 订单 id
    String order_id;

    // 优惠券使用日期（支付）
    String date_id;

    // 优惠券使用时间（支付）
    String used_time;

    // 历史数据
    String old;

    // 时间戳
    String ts;

}
