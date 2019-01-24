package com.ng.hbase.hbase_mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2FruitMRRunner implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        //得到Job对象
        Job job = Job.getInstance(getConf());
        //指定driver类
        job.setJarByClass(Fruit2FruitMRRunner.class);

        //指定Mapper 和 输入类
        TableMapReduceUtil.initTableMapperJob(
                "fruit",
                new Scan(),
                ReadfruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //指定reduce 和输出类
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                WriteFruitMRReducer.class,
                job);

        //提交
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;

    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Fruit2FruitMRRunner(), args);

        if (code == 0){
            System.out.println("任务正常完成");
        } else {
            System.out.println("任务失败！！！");
        }

    }

}
