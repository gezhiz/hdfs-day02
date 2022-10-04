package com.gezz.hdfs.common.constants;

/**
 * @author gezhizheng
 */
public class GersonConstants {
    /**
     * namenode 主节点
     * 如果需要做到高可用，需要读取zookeeper的主节点配置
     */
    public static String ACTIVE_NAME_NODE_URL = "hdfs://bigdata03:9000";

    public static String KEY_DEFAULTFS = "fs.defaultFS";

    public static String DAY02_PATH = "/Users/gezz/data/bigdata/day02";

    public static String DAY02_PATH_OUT_PUT = "/Users/gezz/data/bigdata/output";

    public static String HDFS_TEST_PATH = "/test";

    public static String HDFS_USER = "bigdata";
}
