package com.xxx.spark.sparkSQL.domain.kafkaJson;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 针对特殊的非标准的JSON格式数据使用的工具
 * 处理三峡三区kafka接口数据。
 */
public class JsonRealUtil {
    /**
     * Json格式的字符串向JavaBean List集合转换，传入空串将返回null
     * 去除5个字段不合格的数据 详见test下的a.json
     * 去掉不合格的数据v=false
     * @param strBody
     * @return
     */
    public static List<JsonPoint> json2ObjectList(String strBody)  {
        List<JsonPoint> list=new ArrayList<>();
        if (strBody == null || "".equals(strBody)) {
            return null;
        }
        else {
            JSONArray jsonArray = JSONArray.parseArray(strBody);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jo = jsonArray.getJSONObject(i);
                if (jo.size()!=4){
                    System.out.println(jo.toString());
                    continue;
                }
                JsonPoint jp=JSONObject.parseObject(jo.toString(),JsonPoint.class);
                if (Objects.equals(jp.getV().trim(),"false")){
                    continue;
                }
                list.add(jp);
            }
            return list;
        }
    }

}