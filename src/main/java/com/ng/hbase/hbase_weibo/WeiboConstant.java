package com.ng.hbase.hbase_weibo;

import org.apache.hadoop.hbase.TableName;

public class WeiboConstant {
    /*所有表的命名空间 */
    public static final String NAMESPACE = "weibo";
    /*内容表的表名*/
    public static final TableName TBL_CONTENT = TableName.valueOf(NAMESPACE + ":tbl_content");
    /*用户关系表的表名*/
    public static final TableName TBL_RELATION = TableName.valueOf(NAMESPACE + ":tbl_relation");
    /*收件箱表的表名*/
    public static final TableName TBL_INBOX = TableName.valueOf(NAMESPACE + ":tbl_inbox");
    public static final String INFO = "info";
    public static final String ATTENDS = "attends";
    public static final String FANS = "fans";
}
