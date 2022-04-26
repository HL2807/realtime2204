package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lqs
 * @Date 2022年04月23日 20:31:33
 * @Version 1.0.0
 * @ClassName UserRegister
 * @Describe dws用户注册累加实体类
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserRegister {

    /** 窗口起始时间 */
    String stt;
    /** 窗口终止时间 */
    String edt;
    /** 注册用户数 */
    Long registerCt;
    /** 时间戳 */
    Long ts;

}
