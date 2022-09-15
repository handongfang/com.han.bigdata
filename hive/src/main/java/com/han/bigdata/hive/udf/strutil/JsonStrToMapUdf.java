package com.han.bigdata.hive.udf.strutil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-07 15:27
 * @Version: 1.0
 * @Company: 58集团
 */
public class JsonStrToMapUdf extends UDF {
    private Logger log = LoggerFactory.getLogger(JsonStrToMapUdf.class.getName());

    public JsonStrToMapUdf() {
    }

    public Map<String, String> evaluate(Object jsonStr) {
        if (jsonStr == null || !(jsonStr instanceof String) || "".equals(jsonStr) || "null".equals(jsonStr.toString().toLowerCase())) {
            return new HashMap<String, String>();
        }
        return JsonStrToMap((String)jsonStr);
    }

    private Map<String, String> JsonStrToMap(String jsonStr) {
        //不希望解析json报错则抓取异常返回null或空Map（最好是空Map，因为SQL中一般转换后直接取值或写入下游表，null值容易出错）
        try {
            Map<String, String> map = new HashMap<String,String>();
            if(jsonStr.startsWith("[{")){
                JSONArray jsonArray = JSONArray.parseArray(jsonStr);
                //如果是json数组，则递归调用解析内部json（递归是担心内部还有json数组）
                for (Object str : jsonArray) {
                    map.putAll(JsonStrToMap(str.toString()));
                }
            }else{
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                //map = jsonObject.toJavaObject(Map.class);
                map.putAll(jsonObject.toJavaObject(Map.class));
            }
            return map;
        } catch (Exception e) {
            log.error("json转换map失败:jsonStr=> {},Exception:{}", jsonStr, e.getMessage());
            return new HashMap<String, String>();
        }
    }

    public static void main(String[] args) {
        JsonStrToMapUdf jsonStrToMapUdf = new JsonStrToMapUdf();
        Map<String, String> map = jsonStrToMapUdf.evaluate("{\"1\":\"A\",\"2\":\"B\",\"3\":\"C\"}");
        System.out.println(map);
        System.out.println("---------------------------------");
        Map<String, String> map2 = jsonStrToMapUdf.evaluate("[{\"1\":\"A\",\"2\":\"B\",\"3\":\"C\"},{\"1\":\"Aa\",\"2\":\"B\",\"4\":\"C\"}]");
        System.out.println(map2);
    }
}
