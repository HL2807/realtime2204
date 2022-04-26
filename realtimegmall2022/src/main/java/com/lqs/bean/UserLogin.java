package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lqs
 * @Date 2022年04月23日 21:08:21
 * @Version 1.0.0
 * @ClassName UserLogin
 * @Describe 用户登录和回流用户求和实体类
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserLogin {

    /** 窗口起始时间 */
    String stt;
    /** 窗口终止时间 */
    String edt;
    /** 回流用户数 */
    Long backCt;
    /** 独立用户数 */
    Long uuCt;
    /** 时间戳 */
    Long ts;

}
