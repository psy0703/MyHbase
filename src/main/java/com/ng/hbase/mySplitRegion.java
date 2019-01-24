package com.ng.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 使用 JavaAPI 创建预分区
 */
public class mySplitRegion  {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "psy831");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("staff4"));
        HColumnDescriptor hd = new HColumnDescriptor(Bytes.toBytes("info"));
        desc.addFamily(hd);

        byte[][] splits = {
                Bytes.toBytes("1000"),
                Bytes.toBytes("2000"),
                Bytes.toBytes("3000"),
                Bytes.toBytes("4000")
        };
        admin.createTable(desc, splits);

        connection.close();
    }
}
