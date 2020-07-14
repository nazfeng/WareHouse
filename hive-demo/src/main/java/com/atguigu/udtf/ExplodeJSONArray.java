package com.atguigu.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * [{"a":"a1"},{"b","b1"},{"c":"c1"}]
 */
public class ExplodeJSONArray extends GenericUDTF {

    //声明炸开数据的默认列名和类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //列名集合
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("action");

        //列类型校验器集合
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //遍历每一行数据,做炸开操作【多次写出操作】
    //[{"a":"a1"},{"b":"b1"},{"c":"c1"}]
    @Override
    public void process(Object[] args) throws HiveException {

        if (args.length <= 0) {
            return;
        }

        //1.获取UDTF函数的输入数据:[{"a":"a1"},{"b","b1"},{"c":"c1"}]
        if (args[0] == null) {
            return;
        }

        String input = args[0].toString();

        //3.对输入数据创建JSON数组
        JSONArray jsonArray = new JSONArray(input);

        //4.遍历JSON数组,将每一个元素写出
        for (int i = 0; i < jsonArray.length(); i++) {

            ArrayList<Object> objects = new ArrayList<Object>();
            objects.add(jsonArray.getString(i));

            forward(objects);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
