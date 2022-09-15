package com.han.bigdata.hive.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-09-15 14:06
 * @Version: 1.0
 * @Company: 58集团
 */
public class JsonStrDemo {
 public static void main(String[] args) {
  String json = "{\"array\":[{\"1\":\"A\",\"2\":\"B\",\"3\":\"C\"},{\"1\":\"Aa\",\"2\":\"B\",\"4\":\"C\"}]}";
  JSONObject jsonObject = JSON.parseObject(json);
  System.out.println(jsonObject);
 }
}
