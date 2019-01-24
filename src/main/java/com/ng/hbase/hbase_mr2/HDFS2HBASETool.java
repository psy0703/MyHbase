package com.ng.hbase.hbase_mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFS2HBASETool implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HDFS2HBASETool.class);

        //设置 Mapper
        job.setMapperClass(HDFS2HBASEMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit2",

                HDFS2HBASEReducer.class,
                job);
        job.setNumReduceTasks(1);

        boolean result = job.waitForCompletion(true);
        return result? 0 : 1;

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
        int code = ToolRunner.run(new HDFS2HBASETool(), args);
        System.out.println(code == 0 ? "执行成功" : "执行失败");
    }
}
