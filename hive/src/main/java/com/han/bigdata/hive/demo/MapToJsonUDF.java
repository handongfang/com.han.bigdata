package com.han.bigdata.hive.demo;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-09-15 11:10
 * @Version: 1.0
 * @Company: 58集团
 */
public class MapToJsonUDF extends UDF {
    public MapToJsonUDF(){

    }
    public Text evaluate(Map<String, String> map) {
        if(map==null) return new Text("{}");
        return new Text(JSON.toJSONString(map)) ;
    }

    public static void main(String[] args) {
        MapToJsonUDF mapToJsonUDF = new MapToJsonUDF();
        Map<String, String> map = new HashMap<String,String>();
        map.put("A","1"); map.put("B","2");
        System.out.println(mapToJsonUDF.evaluate(map));
    }
}
