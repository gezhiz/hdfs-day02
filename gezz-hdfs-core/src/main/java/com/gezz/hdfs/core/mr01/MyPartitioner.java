package com.gezz.hdfs.core.mr01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author LIAO
 * create  2020-11-16 20:35
 * Partitioner<KEY, VALUE>
 * KEY:单词   Text类型
 * VALUE:次数  LongWritable类型
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        //根据单词的长度进行判断   单词长度>=5的在一个文件中，<5的在一个文件中
        if (text.toString().length() >= 3) {
            return 0;
        } else {
            return 1;
        }
    }
}
