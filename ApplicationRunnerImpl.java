package com.lzx946479362.collection.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ApplicationRunnerImpl implements ApplicationRunner {
    @Autowired
    private BatchService batchService;


    //传参数
    @Value("${hzwx.url2.password}")
    private String password;

    @Value("${hzwx.url2.username}")
    private String username;

    @Value("${hzwx.url2.grant_type}")
    private String grant_type;

    @Value("${hzwx.url2.scope}")
    private String scope;

    @Value("${hzwx.url.current}")
    private String current;

    @Value("${hzwx.url.size}")
    private String size;

    @Value("${hzwx.url.startTime}")
    private String startTime;

    @Value("${hzwx.url.endTime}")
    private String endTime;

    @Value("${hzwx.Authorization_Post}")
    private String Authorization;

    @Value("${hzwx.Content-Type}")
    private String CT;


    @Override
    public void run(ApplicationArguments args) throws Exception {
        batchService.batchGetData(username, password, grant_type, scope, current, size, startTime, endTime, Authorization, CT);
    }
}
