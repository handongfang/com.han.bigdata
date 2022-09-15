package com.han.bigdata.hive.udf.maputil;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-07 16:50
 * @Version: 1.0
 * @Company: 58集团
 */
public class MapMergeUdf extends UDF {
    private Logger log = LoggerFactory.getLogger(MapMergeUdf.class.getName());

    public MapMergeUdf() {
    }

    //传入多个参数
    public Map<String, String> evaluate(Object[] arguments) {
        Map<String, String> map = new HashMap<String, String>();
        if (null == arguments || arguments.length < 1) {
            return map;
        } else if (1 == arguments.length) {
            if(arguments[0] != null && arguments[0] instanceof Map){
                Map<String, Object> argument = (Map<String, Object>) arguments[0];
                return com.han.bigdata.hive.udf.maputil.MapUtil.mapKVTransform(argument);
            }else{
                return map;
            }
        } else {
            try {
                map = mapMerge(arguments);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    private Map<String, String> mapMerge(Object[] arguments) {
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < arguments.length; ++i) {
            if (arguments[i] != null && arguments[i] instanceof Map) {
                Map<String, Object> map01 = (Map<String, Object>) arguments[i];
                Map<String, String> map02 = com.han.bigdata.hive.udf.maputil.MapUtil.mapKVTransform(map01); //值额外的处理（比如清洗部分歧义值）
                Set<Map.Entry<String, String>> entrySet = map02.entrySet();
                Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();

                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String key = entry.getKey();
                    if (null != key && !"".equals(key) && !"null".equalsIgnoreCase(key)) {
                        String value = entry.getValue().toString();
                        map.put(key, value);
                    }
                }
            }
        }

        return map;
    }

    public static void main(String[] args) throws JSONException {
        JSONObject jsonObject = JSONObject.parseObject("{\"nettype\":\"wifi\",\"uniqueid\":\"ca337ea5be29551ffb7dc9bc1141035c\",\"mac\":\"94:92:BC:DB:08:A6\",\"currentcid\":\"505\",\"official\":\"true\",\"idfa\":\"-\"}");
        Map<String, String> map1 = jsonObject.toJavaObject(Map.class);
        jsonObject = JSONObject.parseObject("{\"CLICKID\":\"20905032-d535-4b04-9f24-ea7eb99ee08b\",\"GTID\":\"54cb0f81-5cd9-40e6-aa65-e80231429e9a\",\"PGTID\":\"0576ea80-efe5-4309-8d40-773791068cae\",\"sidDict\":{\"PGTID\":\"150834696198217116961581832\",\"GTID\":\"138741997198217116968252938\",\"userType\":1,\"isTelSecret\":0,\"onlyWeiLiao\":0},\"PCLICKID\":\"725ddac4-cfad-4671-86dd-9d76ea85444c\"}");
        Map<String, String> map2 = jsonObject.toJavaObject(Map.class);
        MapMergeUdf mmu = new MapMergeUdf();
        Map c = mmu.mapMerge(new Object[]{map1, map2});
        System.out.println(c);
    }
}
