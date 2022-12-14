package com.gezz.hdfs.core.day02;


import com.gezz.hdfs.common.constants.GersonConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 合并下载小文件
 * @author LIAO
 */
public class HdfsMergeFileDown {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsMergeFileDown hdfsMergeFileDown = new HdfsMergeFileDown();
        hdfsMergeFileDown.mergeFileDown();
    }

    //合并下载到本地磁盘小文件
    public void mergeFileDown() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(GersonConstants.ACTIVE_NAME_NODE_URL), new Configuration(), GersonConstants.HDFS_USER);

        //2、获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //3、获取本地大文件的输出流
        FSDataOutputStream outputStream = localFileSystem.create(new Path(GersonConstants.DAY02_PATH_OUT_PUT + "/test_down.txt"),true);

        //4、获取HDFS下面的所有的小文件
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(GersonConstants.HDFS_TEST_PATH), true);

        //5、遍历
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
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
