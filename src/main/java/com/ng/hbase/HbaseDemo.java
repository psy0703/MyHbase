package com.ng.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {

    private static Configuration conf;
    private static Connection conn;
    private static Admin admin;

    // 为了方便, 我们使用静态代码块来获取 HBaseConfiguration 对象
    static {
        // 创建 Configuration 对象
        conf = HBaseConfiguration.create();
        // 配置 Zookeeper
        conf.set("hbase.zookeeper.quorum", "psy831");
        conf.set("hbase.zookeeper.property.clientPort", "2181");


        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        System.out.println(isTableExists("person"));
        createTable("person", "info");
        System.out.println(isTableExists("person"));
//        deleteTable("person");
//        System.out.println(isTableExists("person"));
        close();
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param cfs       多个列族
     */
    private static void createTable(String tableName, String... cfs) throws IOException {
        if(isTableExists(tableName)) return;  // 表如果存在就直接结束方法
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            descriptor.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(descriptor);
        System.out.println("表:" + tableName + "创建成功!");
    }

    /**
     * 删除指定表名的表
     * 删除前要先disable表，才能删除表
     * @param tableName
     * @throws IOException
     */
    private static void deleteTable(String tableName) throws IOException {
        if (isTableExists(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }else{
            System.out.println("你要删除的表：" + tableName + "不存在！！");
        }
    }

    /**
     * 向表中插入数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param cf 列族
     * @param column 列
     * @param value 值
     * @throws IOException
     */
    private static void addData(String tableName,String rowKey, String cf,
                                String column,String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),
                Bytes.toBytes(column),
                Bytes.toBytes(value));

        table.put(put);
        table.close();

    }

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    private static void deleteRows(String tableName, String...rows) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
       /* //一行行删除
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            table.delete(delete);
        }*/

        //批量删除
        ArrayList<Delete> ds = new ArrayList<>();
        for (String row : rows) {
            ds.add(new Delete(Bytes.toBytes(row)));
        }
        table.delete(ds);
    }


    /**
     * 获取某一行指定列的数据
     * @param tableName
     * @param row
     * @param cf
     * @param c
     * @throws IOException
     */
    private static void getDataByColumn(String tableName,String row,String cf,
    String c) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(row));

        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        for (Cell cell : cells) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            String rowName = Bytes.toString(CellUtil.cloneRow(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            System.out.println("column = " + column);
            System.out.println("rowName = " + rowName);
            System.out.println("value = " + value);
            System.out.println("family = " + family);
        }
    }

    /**
     * 获取某一行数据
     * @param tableName
     * @param row
     * @throws IOException
     */
    private static void getRowData(String tableName, String row) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            String rowName = Bytes.toString(CellUtil.cloneRow(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            System.out.println("column = " + column);
            System.out.println("rowName = " + rowName);
            System.out.println("value = " + value);
            System.out.println("family = " + family);
        }

    }

    /**
     * 获取表中的所有数据
     * @param tableName
     * @throws IOException
     */
    private static void getAllData(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        ResultScanner results = table.getScanner(new Scan());
        for (Result result : results) {
            byte[] row = result.getRow();//行
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));

                String column = Bytes.toString(CellUtil.cloneQualifier(cell));

                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));

                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("value = " + value);
                System.out.println("rowKey = " + rowKey);
                System.out.println("column = " + column);
                System.out.println("family = " + family);
                System.out.println("----------------------------");
            }
        }

    }

    /**
     * 判断指定的表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    private static boolean isTableExists(String tableName) throws IOException {
        // 创建一个连接对象
        boolean isExists = admin.tableExists(TableName.valueOf(tableName));
        return isExists;
    }

    /**
     * 关闭 Admin和 Connection 对象
     */
    private static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
