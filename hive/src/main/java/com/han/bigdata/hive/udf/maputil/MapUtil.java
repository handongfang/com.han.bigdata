package com.han.bigdata.hive.udf.maputil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-07 19:04
 * @Version: 1.0
 * @Company: 58集团
 */
public class MapUtil {
    public MapUtil() {
    }

    public static Map<String, String> mapKVTransform(Map<String,Object> map) {
        Map<String,String> m = new HashMap<String,String>();
        if (null != map && map.size() >= 1) {
            Set<Map.Entry<String, Object>> entrySet = map.entrySet();
            Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry =  iterator.next();
                String key = entry.getKey();
                if (null != key && !"".equals(key) && key.length()>0 && !"null".equalsIgnoreCase(key)) {
                    String value = "-";
                    if (entry.getValue() != null && !"".equals(entry.getValue()) && !"null".equalsIgnoreCase(entry.getValue().toString())) {
                        value = entry.getValue().toString().replaceAll("\t", " ");
                    }

                    m.put(key, value);
                }
            }

            return m;
        } else {
            return new HashMap<String,String>();
        }
    }
}
