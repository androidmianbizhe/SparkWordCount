package com.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

public class MysqlCon {

    public static void main(String[] args) {
        SparkConf conf =  new SparkConf();
        conf.setMaster("local").setAppName("MysqlCon");
        conf.set("spark.sql.shuffle.partitions","1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Map<String,String> map = new HashMap<String,String>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url","jdbc:mysql://13.63.254.129:13308/sharding_db");
        map.put("user","root");
        map.put("password","root");
//        map.put("dbtable","t_src_station_hour_data_origin");
        map.put("dbtable","t_src_station_hour_data");

        Dataset<Row> df = sqlContext.read().options(map).format("jdbc").load();
        df.show();
    }
}
