package com.gezz.hdfs.core.day02;


import com.gezz.hdfs.common.constants.GersonConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 合并上传小文件
 * @author LIAO
 */
public class HdfsMergeFileUpload {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsMergeFileUpload hdfsMergeFileUpload = new HdfsMergeFileUpload();
        hdfsMergeFileUpload.mergeFile();
    }

    /**
     * 合并上传小文件
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(GersonConstants.ACTIVE_NAME_NODE_URL), new Configuration(), GersonConstants.HDFS_USER);

        //2、获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path(GersonConstants.HDFS_TEST_PATH + "/test_big.txt"));

        //3、获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //4、获取本地文件夹下面的所有的小文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path(GersonConstants.DAY02_PATH));

        //5、遍历
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            //6、将小文件给复制到大文件当中
            IOUtils.copy(inputStream,outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        outputStream.flush();
        //7、关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }
}
