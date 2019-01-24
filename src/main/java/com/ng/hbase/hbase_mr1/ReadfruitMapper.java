package com.ng.hbase.hbase_mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadfruitMapper extends TableMapper<ImmutableBytesWritable,Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get()); //拿到行键

        Cell[] cells = value.rawCells();

        for (Cell cell : cells) {
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }
        }

        //写出数据
        context.write(key, put);
    }
}
