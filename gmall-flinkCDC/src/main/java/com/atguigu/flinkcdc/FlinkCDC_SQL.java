package com.atguigu.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建一张表映射到FlinkCDC
        tableEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
                " id INT NOT NULL,\n" +
                " tm_name STRING,\n" +
                " logo_url STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop102',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '000000',\n" +
                " 'database-name' = 'gmall_flink_0625',\n" +
                " 'table-name' = 'base_trademark'\n" +
                ")");

        tableEnv.executeSql("select * from mysql_binlog").print();
    }
}
