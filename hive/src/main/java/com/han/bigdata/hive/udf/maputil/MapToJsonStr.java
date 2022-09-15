package com.han.bigdata.hive.udf.maputil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-14 17:25
 * @Version: 1.0
 * @Company: 58集团
 */
public class MapToJsonStr extends UDF {
    private Logger log = LoggerFactory.getLogger(MapToJsonStr.class.getName());

    public MapToJsonStr() {
    }

    public Text evaluate(Object[] arguments) {
        String JsonStr = "{}";
        if (arguments == null || arguments.length < 1) {
            return new Text(JsonStr);
        } else if (1 == arguments.length) {
            if (arguments[0] != null && arguments[0] instanceof Map) {
                Map<String, Object> argument = (Map<String, Object>) arguments[0];
                Map<String, String> map01 = com.han.bigdata.hive.udf.maputil.MapUtil.mapKVTransform(argument);
                JsonStr = JSONObject.toJSONString(map01);
            }
        } else {
            ArrayList<String> strings = new ArrayList<>();
            Map<String, String> map02 = new HashMap<String, String>();
            for (Object argument : arguments) {
                map02 = com.han.bigdata.hive.udf.maputil.MapUtil.mapKVTransform((Map<String, Object>) argument);
                strings.add(JSONObject.toJSONString(map02));
            }
            JsonStr = strings.toString();
        }
        return new Text(JsonStr);
    }

    public static void main(String[] args) {
        MapToJsonStr mapToJsonStr = new MapToJsonStr();
        HashMap<String, Object> map01 = new HashMap<>();
        map01.put("A", "1");
        HashMap<String, Object> map02 = new HashMap<>();
        map02.put("B", "2");
        map02.put("C", 3);

        Text evaluate = mapToJsonStr.evaluate(new Object[]{map01, map02});
        System.out.println(evaluate.toString());
        JSONArray jsonArray = JSONObject.parseArray(evaluate.toString());
        System.out.println(jsonArray.get(1));

        Text evaluate02 = mapToJsonStr.evaluate(null);
        System.out.println(evaluate02.toString());

    }
}
