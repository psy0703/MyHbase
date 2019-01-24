package com.ng.hbase.hbase_weibo;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static com.ng.hbase.hbase_weibo.WeiboConstant.*;

/**
 * 5) 发布微博内容
 *
 * 6) 添加关注用户
 *
 * 7) 移除（取关）用户
 *
 * 8) 获取关注的人的微博内容
 */
public class Weibo {

    /**
     * 创建命名空间
     */
    public void createNameSpace(){
        WeiboUtils.createNameSpace(WeiboConstant.NAMESPACE);
    }

    /**
     * 创建内容表
     */
    public void createContentTable(){
        byte[][] splitRegion = {
                Bytes.toBytes("01"),
                Bytes.toBytes("02"),
                Bytes.toBytes("03"),
                Bytes.toBytes("04"),
                Bytes.toBytes("05"),
                Bytes.toBytes("06"),
                Bytes.toBytes("07"),
                Bytes.toBytes("08"),
                Bytes.toBytes("09"),
                Bytes.toBytes("10"),
                Bytes.toBytes("11"),
                Bytes.toBytes("12"),
        };

        WeiboUtils.createTable(WeiboConstant.TBL_CONTENT,
                1, splitRegion, WeiboConstant.INFO);
    }

    /**
     * 创建用户关系表
     */
    public void createRelationTable(){
        WeiboUtils.createTable(WeiboConstant.TBL_RELATION,
                1, null,
                WeiboConstant.ATTENDS,WeiboConstant.FANS);
    }

    /**
     * 创建收件箱表
     */
    public void createInboxTable(){
        WeiboUtils.createTable(WeiboConstant.TBL_INBOX,
                3,
                null,
                WeiboConstant.INFO);
    }

    /**
     * 关注用户
     * 1、在用户关系表的uid这一行的，这个列族的attends中添加一列（attendUid）
     * 列名: 被关注人的 uid
     * 列值: 关注的时间
     * 2、在attendUid所在行的列族的FANS 中添加一列（uid）
     * 列名：uid 列值：关注的时间
     * 3、在uid的收件箱表中，从attendUid拉取 最新的3条微博。
     * @param uid 主动关注别人的用户
     * @param attendUid 被关注的用户
     */
    public void attendOthers(String uid,String attendUid){

        Connection conn = WeiboUtils.conn;

        try (Table relationTable = conn.getTable(TBL_RELATION);
             Table inboxTable = conn.getTable(TBL_INBOX);
             Table contentTable = conn.getTable(TBL_CONTENT)){

            // 1. 在用户关系表的uid这一行的, 这个列族attends中添加一列:
            Put put1 = new Put(Bytes.toBytes(uid));
            put1.addColumn(Bytes.toBytes(ATTENDS),
                    Bytes.toBytes(attendUid),
                    Bytes.toBytes(System.currentTimeMillis()));
            relationTable.put(put1);

            // 2. 在attendUid这一行, fans这个列族中也添加一列:
            Put put2 = new Put(Bytes.toBytes(attendUid));
            put2.addColumn(Bytes.toBytes(FANS),
                    Bytes.toBytes(uid),
                    Bytes.toBytes(System.currentTimeMillis()));
            relationTable.put(put2);

            // 3. 把attendUid的最新的3条微博,插入到收件箱中uid这一行
            // 31: 从内容表中扫描数据
            ResultScanner scanner = contentTable.getScanner(new Scan());
            for (Result result : scanner) {
                String row = Bytes.toString(result.getRow());
                // 01_1111_22222
                if (row.split("_")[1].equals(attendUid)){
                    List<Cell> cells = result.listCells();
                    Put put3 = new Put(Bytes.toBytes(uid));
                    put3.addColumn(
                            Bytes.toBytes(INFO),
                            Bytes.toBytes(attendUid),
                            Bytes.toBytes(row));
                    inboxTable.put(put3);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void attentionOthers2(String uid,String attendedUid){
        Connection conn = WeiboUtils.conn;
        try (Table relationTable = conn.getTable(TBL_RELATION);
        Table inboxTable = conn.getTable(TBL_INBOX);
        Table contentTable = conn.getTable(TBL_CONTENT)){

            // 1. 在uid这一行的, 这个列族attends中添加一列:
            Put put1 = new Put(Bytes.toBytes(uid));
            put1.addColumn(Bytes.toBytes(ATTENDS),
                    Bytes.toBytes(attendedUid),
                    Bytes.toBytes(System.currentTimeMillis()));
            relationTable.put(put1);

            // 2. 在attendUid这一行, fans这个列族中也添加一列:
            Put put2 = new Put(Bytes.toBytes(attendedUid));
            put2.addColumn(Bytes.toBytes(FANS),
                    Bytes.toBytes(uid),
                    Bytes.toBytes(System.currentTimeMillis()));
            relationTable.put(put2);

            // 01_20_34320938u40239
            // 3. 把attendUid的最新的3条微博,插入到收件箱中uid这一行
            // 31: 从内容表中扫描数据
            Scan scan = new Scan();
            ByteArrayComparable comparator = new SubstringComparator("_" + attendedUid + "_");
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, comparator);
            scan.setFilter(filter);

            ResultScanner scanner = contentTable.getScanner(scan);
            for (Result result : scanner) {
                String row = Bytes.toString(result.getRow());
                List<Cell> cells = result.listCells();
                Put put3 = new Put(Bytes.toBytes(uid));
                put3.addColumn(Bytes.toBytes(INFO),
                        Bytes.toBytes(attendedUid),
                        Bytes.toBytes(row));
                inboxTable.put(put3);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取所有的粉丝
     * @param uid
     * @return
     */
    private List<byte[]> getAllFans(String uid){
        ArrayList<byte[]> fans = new ArrayList<>(); //存储所有的粉丝的id

        try (Table relationTable = WeiboUtils.conn.getTable(TBL_RELATION)){
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes(FANS));
            Result result = relationTable.get(get);
            List<Cell> cells = result.listCells();

            if (cells != null){
                for (Cell cell : cells) {
                    fans.add(CellUtil.cloneQualifier(cell));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  fans;
    }

    /**
     * 发布微博:
     * 1. 向内容表添加一列:
     * rowkey:
     * 01_uid_ts
     * 列名:
     * "content"
     * 列值:
     * 今天天气不错
     * 2. 向他粉丝的收件箱中推送微博内容(01_uid_ts)
     * a: 找到所有的粉丝
     * b: 向粉丝收件箱中推送
     * @param uid
     * @param content
     */
    public void releaseWeibo(String uid, String content){
        // A: 向内容表写数据
        //1. 生成rowkey
        Calendar calendar = Calendar.getInstance();
        int month = calendar.get(Calendar.MONTH) + 1;//获得当前月

        String rowKey = month + "_" + uid + "_" + calendar.getTimeInMillis();
        //2.将content写入内容表的rowKey中
        try (Table contentTable = WeiboUtils.conn.getTable(TBL_CONTENT)){
            Put contentPut = new Put(Bytes.toBytes(rowKey));
            contentPut.addColumn(Bytes.toBytes(INFO),
                    Bytes.toBytes("content"),
                    Bytes.toBytes(content));
            contentTable.put(contentPut);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // B向收件箱表中插入数据
        // 1. 找到 uid的所有粉丝
        List<byte[]> fans = getAllFans(uid);
        //2.向每个粉丝的收件箱中插入数据
        try (Table inboxTable =WeiboUtils.conn.getTable(TBL_INBOX)){
            for (byte[] fan : fans) {
                Put inboxPut = new Put(fan);
                // 添加一列: 列名是刚发布微博的人的id, 列值: 就是刚刚发布的那条微博的rowkey
                inboxPut.addColumn(Bytes.toBytes(INFO),
                        Bytes.toBytes(uid),
                        Bytes.toBytes(rowKey));
                inboxTable.put(inboxPut);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 显示关注的所有用户
     * @param uid
     */
    public void showMyAttention(String uid){
        System.out.println(uid + " 关注的人： ");
        try (Table relationTable = WeiboUtils.conn.getTable(TBL_RELATION)){
            Get get = new Get(Bytes.toBytes(uid));|
            // 只获取关注的人这一列族的值
            get.addFamily(Bytes.toBytes(ATTENDS));
            Result result = relationTable.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null){
                for (Cell cell : cells) {
                    String attenedUid = Bytes.toString(CellUtil.cloneQualifier(cell));
                    System.out.println(attenedUid);
                }
            }
            System.out.println("------显示完毕-------");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 显示所有的粉丝
     * @param uid
     */
    public void showMyFans(String uid){
        System.out.println(uid + " 的粉丝： ");
        try (Table relationTable = WeiboUtils.conn.getTable(TBL_RELATION)){
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes(FANS));
            Result result = relationTable.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null ){
                for (Cell cell : cells) {
                    String fansId = Bytes.toString(CellUtil.cloneQualifier(cell));
                    System.out.println(fansId);
                }
                System.out.println("-----------显示完毕----------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取关注用户的所有微博
     * @param uid
     */
    public void showMyAttendedWeibo(String uid){
        try (Table inboxTable = WeiboUtils.conn.getTable(TBL_INBOX)){
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes(INFO));
            get.setMaxVersions(3);//获取最进3条微博

            Result result = inboxTable.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null){
                String tmp = "";
                for (Cell cell : cells) {
                    String current = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (! current.equals(tmp)){
                        tmp = current;
                        System.out.println("    --" + tmp + " 的微博");
                    }
                    System.out.println("    --内容" + getContentByRowkey(CellUtil.cloneValue(cell)));

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过rowKey获取内容
     * @param rowKey
     * @return
     */
    private String getContentByRowkey(byte[] rowKey) throws IOException {
        Table table = WeiboUtils.conn.getTable(TBL_CONTENT);

        Get get = new Get(rowKey);
        get.addColumn(Bytes.toBytes(INFO),
                Bytes.toBytes("content"));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        if (cells != null){
            for (Cell cell : cells) {
                return Bytes.toString(CellUtil.cloneValue(cell));
            }
        }
        return "";
    }

    /**
     * 取消关注
     * uid 取消对 cancelAttendedId
     * 1. 在用户关系表中在 uid 这一行, 删除这一列:attends:cancelAttendedId(列名)
     * <p>
     * 2. 在用户关系表中在 cancelAttendedId 这一行, 删除这一列 fans:uid
     * <p>
     * 3. 收件箱: 在uid这一行中 info:cancelAttendedId
     *
     * @param uid
     * @param cancelAttendedId
     */
    public void cancelAttend(String uid, String cancelAttendedId){
        try (Table relationTable = WeiboUtils.conn.getTable(TBL_RELATION);
        Table inboxTable = WeiboUtils.conn.getTable(TBL_INBOX)){
            // 1. 在用户关系表中在 uid 这一行, 删除这一列:attends:cancelAttendedId(列名)
            Delete delete1 = new Delete(Bytes.toBytes(uid));
            delete1.addColumn(Bytes.toBytes(ATTENDS),
                    Bytes.toBytes(cancelAttendedId));

            //2. 在用户关系表中在cancelAttendedId这一行，删除这一列的fans：uid
            Delete delete2 = new Delete(Bytes.toBytes(cancelAttendedId));
            delete2.addColumn(Bytes.toBytes(FANS),
                    Bytes.toBytes(uid));

            relationTable.delete(new ArrayList<>(Arrays.asList(delete1, delete2)));

            //3. 在收件箱表：在uid这一行中 info：cancelAttendedId
            Delete delete3 = new Delete(Bytes.toBytes(uid));

            //删除所有版本
            delete3.addColumns(Bytes.toBytes(INFO),
                    Bytes.toBytes(cancelAttendedId));
            inboxTable.delete(delete3);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
