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
    private Map<String, String> map01;

    public MapAddOrAlter() {
    }

    public Map<String, String> evaluate(Object map, Object[] kv) {
        if (map == null || !(map instanceof Map)) {
            this.map01 = new HashMap<String, String>();
        } else {
            Map<String, Object> argument = (Map<String, Object>) map;
            this.map01 = com.han.bigdata.hive.udf.maputil.MapUtil.mapKVTransform(argument); //值额外的处理（比如清洗部分歧义值）
        }

        int len = kv.length;
        if (len >= 2) {
            if (len % 2 != 0) {
                log.warn("你输入的KV参数非偶数对：len={},将会丢弃最后一个参数：param:{}", len, kv[len - 1].toString());
            }
            String key01;
            String value01;
            for (int index = 0; index < len - 1; index += 2) {
                key01 = "-"; //空值串用'-'
                value01 = "-"; //空值串用'-'
                if (kv[index] != null && (kv[index] instanceof String) && !"".equals(kv[index].toString()))
                    key01 = (String) kv[index];
                if (kv[index + 1] != null && kv[index + 1].toString().length() > 0)
                    value01 = kv[index + 1].toString();
                if (!"-".equals(key01) || !"-".equals(value01)) //不能都为空
                    map01.put(key01, value01);
            }
        } else {
            log.warn("你输入的KV参数过少：len={},不予添加", len);
        }

        return map01;
    }

    public static void main(String[] args) {
        MapAddOrAlter mapAddOrAlter = new MapAddOrAlter();
        Object[] kv1 = new Object[]{"a", "1"};
        Object[] kv2 = new Object[]{"a", "2", "b", 2};
        Object[] kv3 = new Object[]{"a", "2", "b", 2, "c"};
        Map<String, String> evaluate = mapAddOrAlter.evaluate(new HashMap<>(), kv1);
        System.out.println(evaluate);
        System.out.println(mapAddOrAlter.evaluate(evaluate, kv2)); //添加&修改
        System.out.println(mapAddOrAlter.evaluate(evaluate, kv3)); //非偶数队警告
    }
}
