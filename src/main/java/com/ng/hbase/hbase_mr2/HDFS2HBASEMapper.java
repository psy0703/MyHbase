package com.ng.hbase.hbase_mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HDFS2HBASEMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 每行数据使用 \t 切割
        String[] words = value.toString().split("\t");
        // 根据数组中的数据分别取值
        String rowKey = words[0];
        String fruitType = words[1];
        String fruitColor = words[2];

        // 初始化 rowkey
        ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable();

        // 初始化 Put
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name") , Bytes.toBytes(fruitType));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fruitColor));

        context.write(rowKeyWritable, put);
    }
}
