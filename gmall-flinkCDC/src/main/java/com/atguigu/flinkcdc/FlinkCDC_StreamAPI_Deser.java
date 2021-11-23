package com.atguigu.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC_StreamAPI_Deser {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall_flink_0625")
//                .tableList("gmall_flink_0625.z_user_info")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeSer())
                .build();

        env.addSource(sourceFunction).print();

        env.execute();
    }

    public static class MyDeSer implements DebeziumDeserializationSchema<String>{

        /**
         *{
         *   "database":"gmall_flink_0625",
         *   "tableName":"aaa",
         *   "after":{"id":"123","name":"zs"....},
         *   "before":{"id":"123","name":"zs"....},
         *   "type":"insert",
         *}
         */
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //1.创建一个JSONObject用来存放最终封装的数据
            JSONObject jsonObject = new JSONObject();

            //TODO 2.提取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];

            //TODO 3.提取表名
            String tableName = split[2];


            Struct value = (Struct) sourceRecord.value();
            //TODO 4.获取after数据
            Struct valueStruct = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            //判断是否有after
            if (valueStruct!=null){
                List<Field> fields = valueStruct.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(), valueStruct.get(field));
                }
            }

            //TODO 5.获取before数据
            Struct beforeStruct = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            //判断是否有after
            if (beforeStruct!=null){
                List<Field> fields = beforeStruct.schema().fields();
                for (Field field : fields) {
                    beforeJson.put(field.name(), beforeStruct.get(field));
                }
            }

            //TODO 6.获取操作类型DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            String type = operation.toString().toLowerCase();
            if ("create".equals(type)){
                type = "insert";
            }

            //TODO 7.封住数据
            jsonObject.put("database", database);
            jsonObject.put("tableName", tableName);
            jsonObject.put("after", afterJson);
            jsonObject.put("before", beforeJson);
            jsonObject.put("type", type);


            collector.collect(jsonObject.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
