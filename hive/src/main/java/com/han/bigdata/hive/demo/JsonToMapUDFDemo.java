package com.han.bigdata.hive.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.han.bigdata.hive.udf.strutil.JsonStrToMapUdf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;

import java.util.Map;

public class JsonToMapUDFDemo extends UDF {

    public static void main(String[] args) throws JSONException {
        JSONObject jsonObject = JSON.parseObject("{\"a\":1,\"b\":2,\"c\":3}");
        JsonStrToMapUdf json2MapUdf = new JsonStrToMapUdf();
        Map<String, String> evaluate = json2MapUdf.evaluate(jsonObject.toJSONString());

        System.out.println(evaluate);
    }
}
