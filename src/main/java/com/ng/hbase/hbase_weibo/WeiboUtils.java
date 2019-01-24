package com.ng.hbase.hbase_weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WeiboUtils {
    private static Configuration conf;
    public static Connection conn;

    static {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop201");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            // 1. 先有连接
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建指定的命名空间
     *
     * @param nameSpace 命名空间的名字
     */
    public static void createNameSpace(String nameSpace) {
        if (exsistNameSpace(nameSpace)) {
            return;
        }
        //1. 连接 2. admin 3. 创建
        // 1. 获取admin对象
        try (Admin admin = conn.getAdmin()) {
            admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean exsistNameSpace(String nameSpace) {
        try (Admin admin = conn.getAdmin()) {
            admin.getNamespaceDescriptor(nameSpace);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * 创建一个表
     * @param tableName 表名
     * @param versions 保存的最大的版本数
     * @param splitRegion 分区
     * @param cfs 列族
     */
    public static void createTable(TableName tableName, int versions, byte[][] splitRegion, String... cfs) {
        if (exsistTable(tableName)) return;
        //1. get到admin
        try (Admin admin = conn.getAdmin()) {

            HTableDescriptor desc = new HTableDescriptor(tableName);
            for (String cf : cfs) {
                HColumnDescriptor cdesc = new HColumnDescriptor(Bytes.toBytes(cf));
                cdesc.setMaxVersions(versions);
                desc.addFamily(cdesc);
            }
            admin.createTable(desc, splitRegion);
        } catch (IOException e) {

            e.printStackTrace();
        }

    }

    private static boolean exsistTable(TableName tableName) {
        try (Admin admin = conn.getAdmin()) {
            return admin.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
