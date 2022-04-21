package com.lqs.bean;

import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月21日 16:13:52
 * @Version 1.0.0
 * @ClassName CouponUseOrderBean
 * @Describe 工具域优惠券领用事实表
 */

@Data
public class CouponUseOrder {

    // 优惠券领用记录 id
    String id;

    // 优惠券 id
    String coupon_id;

    // 用户 id
    String user_id;

    // 订单 id
    String order_id;

    // 优惠券使用日期（下单）
    String date_id;

    // 优惠券使用时间（下单）
    String using_time;

    // 历史数据
    String old;

    // 时间戳
    String ts;

}
