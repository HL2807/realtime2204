package com.lqs.gmall2204publisher.contorller;

import com.lqs.gmall2204publisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;

/**
 * @Author lqs
 * @Date 2022年04月28日 19:30:45
 * @Version 1.0.0
 * @ClassName SugarController
 * @Describe
 */

//@Controller
@RestController // = @Controller+@ResponseBody
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private GmvService gmvService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        //查询数据
        Double gmv = gmvService.getGmv(date);

        //拼接并返回结果数据
        return "{ " +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": " + gmv +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }

}
