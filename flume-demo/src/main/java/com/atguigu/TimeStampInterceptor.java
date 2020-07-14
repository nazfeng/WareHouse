package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeStampInterceptor implements Interceptor {

    private List<Event> result = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1.获取事件的头信息和Body信息
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String bodyStr = new String(body);

        //2.将bodyStr转换为JSON对象
        JSONObject jsonObject = JSON.parseObject(bodyStr);

        //3.获取数据中的时间戳
        String ts = jsonObject.getString("ts");

        //4.用数据中的时间戳替换headers中的时间戳
        headers.put("timestamp", ts);

        //5.返回数据
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        //1.清空集合
        result.clear();

        //2.遍历list,给每条数据添加时间戳
        for (Event event : list) {
            result.add(intercept(event));
        }

        //3.返回结果
        return result;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
