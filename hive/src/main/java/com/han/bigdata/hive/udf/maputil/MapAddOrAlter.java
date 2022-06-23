package com.han.bigdata.hive.udf.maputil;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-14 16:33
 * @Version: 1.0
 * @Company: 58集团
 */
public class MapAddOrAlter extends UDF {
    private Logger log = LoggerFactory.getLogger(MapAddOrAlter.class.getName());

    public MapAddOrAlter() {
    }

    public Map<String, String> evaluate(Object map, Object key, Object value) {
        if (map == null || !(map instanceof Map)) {
            return new HashMap<String, String>();
        }
        Map<String, Object> argument = (Map<String, Object>) map;
        Map<String, String> map01 = com.han.bigdata.hive.udf.maputil.MapUtil.mapKVTransform(argument); //值额外的处理（比如清洗部分歧义值）
        if (key != null && (key instanceof String) && !"".equals(key.toString())) {
            String key01 = (String) key;
            String value01 = "-"; //空值/串用'-'
            if (value != null && value.toString().length() > 0) {
                value01 =  value.toString();
            }
            map01.put(key01, value01);
        }
        return map01;
    }

    public static void main(String[] args) {
        MapAddOrAlter mapAddOrAlter = new MapAddOrAlter();
        Map<String, String> evaluate = mapAddOrAlter.evaluate(new HashMap<>(), "a", "1");
        System.out.println(mapAddOrAlter.evaluate(evaluate, "b", 2)); //添加
        System.out.println(mapAddOrAlter.evaluate(evaluate, "a", 2)); //修改
    }
}
