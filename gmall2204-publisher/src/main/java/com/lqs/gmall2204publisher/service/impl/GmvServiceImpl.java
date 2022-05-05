package com.lqs.gmall2204publisher.service.impl;

import com.lqs.gmall2204publisher.mapper.GmvMapper;
import com.lqs.gmall2204publisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author lqs
 * @Date 2022年04月28日 19:33:46
 * @Version 1.0.0
 * @ClassName GmvServiceImpl
 * @Describe
 */

@Service
public class GmvServiceImpl implements GmvService {

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.selectGmv(date);
    }

}
