package com.fyp.engine.common.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSSupport {

    private static Logger LOGGER = LoggerFactory.getLogger(HDFSSupport.class);

    /**
     * 读取hdfs bitmap文件数据
     *
     * @param fileName
     * @return
     */
    public static RoaringBitmap readHDFSFileForBitmap(String fileName) throws Exception {
        FSDataInputStream inputStream = null;
        RoaringBitmap     bitmap      = new RoaringBitmap();
        try {
            FileSystem fileSystem = HDFSDatasource.getInstance().getFileSystem();
            Path       path       = new Path(fileName);
            FileStatus fsStatus   = fileSystem.getFileStatus(path);
            // the path point to a file
            if (!fsStatus.isFile()) {
                // the path point to a dir
                LOGGER.error("HDFS路径[{}]为文件夹，不处理", fileName);
                throw new RuntimeException("暂不处理非文件格式的hdfs路径: " + path.toString());
            }
            inputStream = fileSystem.open(path);
            bitmap.deserialize(inputStream);
        } catch (Exception e) {
            LOGGER.error("读取HDFS路径报错，HDFS路径为:" + fileName, e);
            throw e;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        return bitmap;

    }

    public static String readArgs(String fileName) throws IOException {
        try {
            return HDFSDatasource.getInstance().readHDFSFileForString(fileName);
        } catch (Exception e) {
            LOGGER.error("读取HDFS路径报错，HDFS路径为:" + fileName, e);
            throw e;
        }
    }
}
