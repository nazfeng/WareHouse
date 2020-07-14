package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.jute.compiler.JString;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author NaZFeng
 * @create 2020-06-29 17:55
 */
public class JsonArrayToStructArray extends GenericUDF {

    private List<List<Object>>  result = new ArrayList<List<Object>>();
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length< 3 ){
            throw new UDFArgumentException("json_array_to_struct_array需要至少3个参数");
        }
        for (int i = 0; i < arguments.length; i++) {
            if (!"string".equals(arguments[i].getTypeName())){
                throw new UDFArgumentException("json_array_to_struct_array的第\" + (i + 1) + \"个参数应为string类型");
            }
        }


        List<String> fieldNames = new ArrayList<String>() ;
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        for (int i = 1+(arguments.length-1)/2; i <arguments.length; i++) {
            if (!(arguments[i] instanceof ConstantObjectInspector)){
                throw new UDFArgumentException("参数错误");
            }
            String field = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue().toString();

            String[] split = field.split(":");

            fieldNames.add(split[0]);

            switch (split[1]){
                case "string":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    break;
                case "boolean":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
                    break;
                case "tinyint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
                    break;
                case "smallint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
                    break;
                case "int":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                    break;
                case "bigint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                    break;
                case "float":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
                    break;
                case "double":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                    break;
                default:
                    throw new UDFArgumentException("json_array_to_struct_array 不支持" + split[1] + "类型");
            }



        }

        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs));
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject data = arguments[0];

        if (data == null){
            return null;
        }

        String line = data.get().toString();

        result.clear();

        JSONArray jsonArray = new JSONArray(line);

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject json = jsonArray.getJSONObject(i);

            List<Object> struct = new ArrayList<>();

            for (int j = 0; j <1+ (arguments.length-1)/2; j++) {
                String key = arguments[j].get().toString();
                if (json.has(key)){
                    struct.add(json.getString(key));
                }else {
                    struct.add(null);
                }
            }
            result.add(struct);
        }

        return result;
    }

    public String getDisplayString(String[] children) {
        return getStandardDisplayString("JsonArrayToStructArray",children,",");

    }
}
