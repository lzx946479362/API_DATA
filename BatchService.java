package com.lzx946479362.collection.api;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;


@Slf4j
@Service
class BatchService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static String JOB_TYPE = "lzx946479362-api";

    private static String STATTUS_FAILED = "9";

    private static String STATTUS_SUCCESS = "1";

    private static List<Object[]> paramList = new ArrayList<>();

    private static String table = "lzx946479362";

    private static SimpleDateFormat daysdf;


    //获取数据
    public void batchGetData(String username, String password, String grant_type, String scope, String current, String size, String startTime, String endTime, String Authorization, String CT) {
        LocalDateTime execStartTime_1 = LocalDateTime.now();

        //没有指定日期就获取前一天日期
        daysdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        if (StringUtils.isEmpty(startTime)) {
            startTime = daysdf.format(calendar.getTime()) + " 00:00:00";
        }
        if (StringUtils.isEmpty(endTime)) {
            endTime = daysdf.format(calendar.getTime()) + " 23:59:59";
        }
        try {
            // 获取token接口的JSON
            String url_getToken = "https://lzx946479362.com/auth/oauth/token?username=" + username + "&password=" + password + "&grant_type=" + grant_type + "&scope=" + scope;
            HttpHeaders headers_getToken = new HttpHeaders();
            headers_getToken.add("Authorization", Authorization);
            HttpEntity<JSONObject> entity_getToken = new HttpEntity<>(headers_getToken);
            RestTemplate restTemplate_getToken = new RestTemplate();
            ResponseEntity<String> exchange = restTemplate_getToken.exchange(url_getToken, HttpMethod.POST, entity_getToken, String.class);
            Map<String, Object> data_getToken = JSONObject.parseObject(exchange.getBody(), Map.class);

            // 请求数据
            HttpHeaders headers_getData = new HttpHeaders();
            headers_getData.add("Authorization", data_getToken.get("token_type") + "" + data_getToken.get("access_token"));
            headers_getData.add("Content-Type", CT);
            HttpEntity entity_getData = new HttpEntity(headers_getData);
            RestTemplate restTemplate_getData = new RestTemplate();
            String url_getData = "https://lzx946479362.com/datacenter/iprskudetail/external/page?current=" + current + "&size=" + size + "&startTime=" + startTime + "&endTime=" + endTime;
            ResponseEntity<String> forEntity = restTemplate_getData.exchange(url_getData, HttpMethod.GET, entity_getData, String.class);
            log.info("body:" + forEntity.getBody());
            JSONObject obj = JSONObject.parseObject(forEntity.getBody());

            //JSON数据解析,获取到JSON数组
            JSONObject dataObj = obj.getJSONObject("data");
            JSONArray recordsObjs = dataObj.getJSONArray("records");

            //先清空数据库表数据再进行数据插入
//            deletePromotion(table);
            batchSaveData(recordsObjs, startTime);
        } catch (Exception e) {
            String errMsg = getErrorMsg(e);
            recordLogs(table, JOB_TYPE, execStartTime_1, errMsg, STATTUS_FAILED);
            log.error("数据保存失败:", e);
            return;
        }
//        System.out.println(startTime);
//        System.out.println(endTime);
    }


    //清空数据库的表数据
    public void deletePromotion(String table) {
        String delSql = "truncate table `" + table + "`";
        jdbcTemplate.update(delSql);
    }


    //获取到的数据入库
    public void batchSaveData(JSONArray recordsObjs, String startTime) {
        LocalDateTime execStartTime_2 = LocalDateTime.now();
        //把获取到的数据插入表中
        String insertSql = "INSERT INTO `" + "lzx946479362" + "` (  `field_1`, `field_2`, `field_3`, `field_4`, `field_5`, `field_6` , `field_7`, `field_8`, `field_9`, `field_10`, `field_11`, `field_12`, `field_13`, `field_14` , `field_15`, `field_16`, `field_17`, `field_18`, `field_19`, `field_20`, `field_21` , `field_22`  ) VALUES( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

        //对JSON数组进行遍历,得到JSON数组内每个JSON对象中数据组成的数组
        for (int i = 0; i < recordsObjs.size(); i++) {
            JSONObject result = recordsObjs.getJSONObject(i);
            Object[] arr = new Object[22];
            arr[0] = result.get("field_1");
            arr[1] = result.get("field_2");
            arr[2] = result.get("field_3");
            arr[3] = result.get("field_4");
            arr[4] = result.get("field_5");
            arr[5] = result.get("field_6");
            arr[6] = result.get("field_7");
            arr[7] = result.get("field_8");
            arr[8] = result.get("field_9");
            arr[9] = result.get("field_10");
            arr[10] = result.get("field_11");
            arr[11] = result.get("field_12");
            arr[12] = result.get("field_13");
            arr[13] = result.get("field_14");
            arr[14] = result.get("field_15");
            arr[15] = result.get("field_16");
            arr[16] = result.get("field_17");
            arr[17] = result.get("field_18");
            arr[18] = result.get("field_19");
            arr[19] = result.get("field_20");
            arr[20] = result.get("field_21");
            arr[21] = result.get("field_22");
            paramList.add(arr);
        }
        log.info("monitorTime:" + startTime.substring(0, 10) + ",size:" + paramList.size());

        //连接Mysql数据库,向表中插入数据
        jdbcTemplate.batchUpdate(insertSql, paramList);
        log.info("数据保存成功,count:" + paramList.size());
        String successMsg = "监控日期:" + startTime.substring(0, 10) + "；该日总记录数:" + paramList.size() + "条";
        recordLogs(table, JOB_TYPE, execStartTime_2, successMsg, STATTUS_SUCCESS);
    }

    //记录日志
    private void recordLogs(String jobId, String jobType, LocalDateTime startTime_10, String message, String status) {
        String sql = "INSERT INTO api_job_info (  `JOB_ID`, `JOB_TYPE`, `job_status`, `message`, `start_time`, `END_Time`) VALUES( ?,?,?,?,?,?);";
        List<Object[]> paramList = new ArrayList<>();
        Object[] arr = new Object[6];
        arr[0] = jobId;
        arr[1] = jobType;
        arr[2] = status;
        arr[3] = message;
        arr[4] = startTime_10;
        arr[5] = LocalDateTime.now();
        paramList.add(arr);
        jdbcTemplate.batchUpdate(sql, paramList);
    }

    private String getErrorMsg(Exception e) {
        return e.getMessage().length() > 1000 ? e.getMessage().substring(1000) : e.getMessage();
    }
}
