package com.fyp.engine.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class HDFSDatasource {

    private static final Integer        BUFFER_SIZE    = 8096;
    private static       HDFSDatasource hdfsDatasource = new HDFSDatasource();
    private static       Logger         LOGGER         = LoggerFactory.getLogger(HDFSDatasource.class);
    private              FileSystem     fileSystem;
    private              Configuration  conf;

    public HDFSDatasource() {
        Properties ps                   = PropertiesUtil.getProperties("hdfs");
        String     defaultFs            = ps.getProperty("fs.defaultFS");
        String     zkQuorom             = ps.getProperty("ha.zookeeper.quorum");
        String     nameServices         = ps.getProperty("dfs.nameservices");
        String     nameNode01           = ps.getProperty("dfs.namenode01");
        String     nameNode02           = ps.getProperty("dfs.namenode02");
        String     nameNode01RpcAddress = ps.getProperty("dfs.namenode01.rpc-address");
        String     nameNode02RpcAddress = ps.getProperty("dfs.namenode02.rpc-address");
        String     hadoopUser           = ps.getProperty("hadoop.user");

        conf = new Configuration();
        conf.set("fs.defaultFS", defaultFs);
        conf.set("ha.zookeeper.quorum", zkQuorom);

        conf.set("dfs.nameservices", nameServices);
        conf.set("dfs.ha.namenodes." + nameServices, nameNode01 + "," + nameNode02);
        conf.set("dfs.ha.namenodes." + nameServices, nameNode01 + "," + nameNode02);
        conf.set("dfs.namenode.rpc-address." + nameServices + "." + nameNode01, nameNode01RpcAddress);
        conf.set("dfs.namenode.rpc-address." + nameServices + "." + nameNode02, nameNode02RpcAddress);
        conf.set("dfs.client.failover.proxy.provider." + nameServices, ConfiguredFailoverProxyProvider.class.getCanonicalName());
        System.setProperty("HADOOP_USER_NAME", hadoopUser);

        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            LOGGER.error("连接fileSystem异常", e);
        }
    }

    /**
     * 单例获取对象
     *
     * @return
     */
    public static HDFSDatasource getInstance() {
        return hdfsDatasource;
    }

    /**
     * 获取fileSystem
     *
     * @return
     */
    public FileSystem getFileSystem() throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(conf);
        }
        return fileSystem;
    }

    public FSDataOutputStream getOutputStream(String fileName) {
        try {
            FSDataOutputStream fsOutStream;
            Path               path = new Path(fileName);
            if (fileSystem.exists(path)) {
                fsOutStream = fileSystem.append(path);
            } else {
                fsOutStream = fileSystem.create(path);
            }
            return fsOutStream;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 获取某个目录下面的文件（不包含文件夹）
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<FileStatus> getHDFSFileList(String path) {
        try {
            fileSystem = getFileSystem();
            FileStatus[] fileStatusList = fileSystem.listStatus(new Path(path));
            if (null == fileStatusList || 0 == fileStatusList.length) {
                LOGGER.warn("当前HDFS目录:" + path + "路径下文件为空!");
                return null;
            }
            List<FileStatus> fileList = new ArrayList<>();
            for (FileStatus file : fileStatusList) {
                if (file.isFile()) {
                    fileList.add(file);
                }
            }
            return fileList;

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 获取最新的修改的文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    public FileStatus getHDFSLastestFile(String path) {

        List<FileStatus> fileList = getHDFSFileList(path);
        if (fileList != null) {
            Collections.sort(fileList, new Comparator<FileStatus>() {

                @Override
                public int compare(FileStatus o1, FileStatus o2) {
                    if (o1.getModificationTime() > o2.getModificationTime()) {
                        return -1;
                    } else if (o1.getModificationTime() < o2.getModificationTime()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });
            return fileList.get(0);
        }
        return null;
    }

    /**
     * 读取hdfs文件数据
     *
     * @param fileName
     * @return
     */
    public List<String> readHDFSFile(String fileName) {
        List<String> res = new ArrayList();
        try {
            fileSystem = getFileSystem();
            Path           path     = new Path(fileName);
            FileStatus     fsStatus = fileSystem.getFileStatus(path);
            BufferedReader br;
            String         line;
            // the path point to a file
            if (fsStatus.isFile()) {
                FSDataInputStream inputStream = fileSystem.open(path);
                br = new BufferedReader(new InputStreamReader(inputStream));

                while (StringUtils.isNotBlank(line = br.readLine())) {
                    line = new String(line.getBytes(), "utf-8");
                    res.add(StringUtils.trim(line));
                }
                br.close();
            } else if (fsStatus.isDirectory()) {
                // the path point to a dir
                for (FileStatus file : fileSystem.listStatus(path)) {
                    FSDataInputStream inputStream = fileSystem.open(file.getPath(), 1024 * 8);
                    br = new BufferedReader(new InputStreamReader(inputStream));
                    while (StringUtils.isNotBlank(line = br.readLine())) {
                        line = new String(line.getBytes(), "utf-8");
                        res.add(line);
                    }
                    br.close();
                }
            }
        } catch (Exception e) {
            LOGGER.error("读取HDFS路径报错，HDFS路径为:" + fileName, e);
        }
        return res;

    }

    /**
     * 读取hdfs文件数据
     *
     * @param fileName
     * @return
     */
    public String readHDFSFileForString(String fileName) throws IOException {
        StringBuilder     sb          = new StringBuilder();
        FSDataInputStream inputStream = null;
        BufferedReader    br          = null;
        try {
            fileSystem = getFileSystem();
            Path       path     = new Path(fileName);
            FileStatus fsStatus = fileSystem.getFileStatus(path);
            String     line;
            // the path point to a file
            if (fsStatus.isFile()) {
                inputStream = fileSystem.open(path);
                br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

                while (StringUtils.isNotBlank(line = br.readLine())) {
                    line = new String(line.getBytes(Charset.forName("UTF-8")), StandardCharsets.UTF_8);
                    sb.append(line);
                }
            }
        } catch (Exception e) {
            LOGGER.error("读取HDFS路径报错，HDFS路径为:" + fileName, e);
        } finally {
            if (br != null) {
                br.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        }
        return sb.toString();
    }

    /**
     * 生成HDFS文件
     *
     * @param outputStream
     * @param data
     * @return
     */
    public boolean addHDFSData4Self(FSDataOutputStream outputStream, String data) {
        try {
            byte[] byt = (data + "\n").getBytes();
            outputStream.write(byt);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    public boolean createHdfsFile(String fileName) {
        try {
            fileSystem = getFileSystem();
            Path path = new Path(fileName);
            if (!fileSystem.exists(path)) {
                FSDataOutputStream fsOutStream = fileSystem.create(path);
                fsOutStream.close();
            }
        } catch (Exception e) {
            LOGGER.error("创建HDFS文件失败，路径为:" + fileName + ",原因为:" + e.getMessage(), e);
            return false;
        }
        return true;
    }

    /**
     * 生成HDFS文件
     *
     * @param fileName
     * @param data
     * @return
     */
    public boolean addHDFSData(String fileName, String data) {
        try {
            fileSystem = getFileSystem();
            FSDataOutputStream fsOutStream = null;
            Path               path        = new Path(fileName);
            if (fileSystem.exists(path)) {
                fsOutStream = fileSystem.append(path);
            } else {
                try {
                    fsOutStream = fileSystem.create(path);
                } catch (FileAlreadyExistsException e) {
                    fsOutStream = fileSystem.append(path);
                }
            }
            byte[] byt = data.getBytes();
            fsOutStream.write(byt);
            fsOutStream.close();
        } catch (Exception e) {
            LOGGER.error("增加HDFS文件失败，路径为:" + fileName + ",原因为:" + e.getMessage(), e);
            return false;
        }
        return true;
    }

    /**
     * 往文件写多行内容
     *
     * @param filePath
     * @param dataList
     * @throws IOException
     */
    public void addHDFSDataList(String filePath, List<String> dataList) throws IOException {
        fileSystem = getFileSystem();
        FSDataOutputStream fsOutStream = null;
        Path               path        = new Path(filePath);
        if (fileSystem.exists(path)) {
            fsOutStream = fileSystem.append(path);
        } else {
            try {
                fsOutStream = fileSystem.create(path);
            } catch (FileAlreadyExistsException e) {
                fsOutStream = fileSystem.append(path);
            }
        }
        if (null != dataList && !dataList.isEmpty()) {
            for (String data : dataList) {
                byte[] byt = (data + "\n").getBytes();
                fsOutStream.write(byt);
            }
        }
        fsOutStream.close();
    }

    /**
     * 生成HDFS文件流形式
     *
     * @param fileName
     * @param in       输入流
     * @return
     */
    public boolean addHDFSData(String fileName, InputStream in) {
        try {
            fileSystem = getFileSystem();
            FSDataOutputStream fsOutStream = null;
            Path               path        = new Path(fileName);
            if (fileSystem.exists(path)) {
                fsOutStream = fileSystem.append(path);
            } else {
                fsOutStream = fileSystem.create(path);
            }
            IOUtils.copyBytes(in, fsOutStream, 4096, true);
        } catch (Exception e) {
            LOGGER.error("增加HDFS文件失败，路径为:" + fileName + ",原因为:" + e.getMessage(), e);
            return false;
        }
        LOGGER.info("增加HDFS文件成功，路径为: {}", fileName);
        return true;
    }

    public boolean checkFilesExists(String fileName) {
        try {
            fileSystem = getFileSystem();
            Path path = new Path(fileName);
            return fileSystem.exists(path);
        } catch (Exception e) {
            LOGGER.error("校验HDFS文件失败，路径为:" + fileName + ",原因为:" + e.getMessage(), e);
            return false;
        }
    }

    /**
     * 切换目录
     *
     * @param fileName
     * @param newFileName
     * @return
     */
    public boolean moveHDFSFile(String fileName, String newFileName) {
        try {
            fileSystem = getFileSystem();
            boolean isSuccess = fileSystem.rename(new Path(fileName), new Path(newFileName));
            if (isSuccess) {
                fileSystem.delete(new Path(fileName), false);
            }
            return isSuccess;
        } catch (Exception e) {
            LOGGER.error("移动HDFS文件失败,路径为:" + fileName + ",目标路径为:" + newFileName, e);
        }
        return true;
    }

    /**
     * 删除文件
     *
     * @param fileName
     * @throws IOException
     */
    public boolean delete(String fileName) {
        try {
            return fileSystem.delete(new Path(fileName), false);
        } catch (Exception e) {
            LOGGER.error("执行HDFS删除失败,文件路径为:" + fileName, e);
        }
        return false;
    }

    /**
     * 计算文件行数
     *
     * @param fileName
     * @return
     */
    public long calFileCount(String fileName) {
        try {
            fileSystem = getFileSystem();
            Path       path     = new Path(fileName);
            FileStatus fsStatus = fileSystem.getFileStatus(path);
            // the path point to a file
            if (fsStatus.isFile()) {
                FSDataInputStream inputStream = fileSystem.open(path);
                BufferedReader    br          = new BufferedReader(new InputStreamReader(inputStream));
                long              count       = 0;
                String            line        = null;
                while (StringUtils.isNotBlank(line = br.readLine())) {
                    count += 1;
                }
                br.close();
                return count;
            } else if (fsStatus.isDirectory()) {
                FileStatus[] status = fileSystem.listStatus(path);
                long         count  = 0;
                for (FileStatus file : status) {
                    if (file.isFile()) {
                        FSDataInputStream hdfsInStream = fileSystem.open(file.getPath(), 1024 * 8);
                        InputStreamReader isr          = new InputStreamReader(hdfsInStream, "utf-8");
                        BufferedReader    br           = new BufferedReader(isr);
                        String            line;
                        while ((line = br.readLine()) != null) {
                            count += 1;
                        }
                    }
                }
                return count;
            }
            return 0L;
        } catch (IOException e) {
            LOGGER.error("执行HDFS读取失败,文件路径为:" + fileName, e);
        }
        return 0L;
    }

    /**
     * 计算文件是否为空
     * 不支持递归
     * @param fileName 文件名/目录
     * @return true:空文件/false:不为空
     */
    public boolean isEmpty(String fileName) {
        try {
            fileSystem = getFileSystem();
            Path       path     = new Path(fileName);
            FileStatus fsStatus = fileSystem.getFileStatus(path);
            if (fsStatus.isFile()) {
                return fsStatus.getLen() == 0L;
            } else if (fsStatus.isDirectory()) {
                for (FileStatus file : fileSystem.listStatus(path)) {
                    if (file.getLen() > 0) {
                        return false;
                    }
                }
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("执行HDFS读取失败,文件路径为:" + fileName, e);
        }
        return true;
    }

    /**
     * 文件路径是否存在
     *
     * @param fileName
     * @throws IOException
     */
    public boolean isExsit(String fileName) {
        boolean result = false;
        try {
            fileSystem = getFileSystem();
            Path path = new Path(fileName);
            if (fileSystem.exists(path)) {
                result = true;
            }
        } catch (Exception e) {
            LOGGER.error("判断路径存在异常，路径为:" + fileName + ",原因为:" + e.getMessage(), e);
            return false;
        }
        return result;
    }

    /**
     * 流式输出
     *
     * @param input
     * @param output
     * @throws Exception
     */
    private void write(InputStream input, OutputStream output) throws Exception {
        byte[] bytes     = new byte[BUFFER_SIZE];
        int    readBytes = 0;
        while ((readBytes = input.read(bytes)) > 0) {
            output.write(bytes, 0, readBytes);
        }
        input.close();
        output.close();
    }

    /**
     * 确认hdfs地址是否为目录
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    public boolean isDirectory(String fileName) {
        try {
            FileStatus fsStatus = fileSystem.getFileStatus(new Path(fileName));
            return fsStatus.isDirectory();
        } catch (IOException e) {
            LOGGER.error("确认HDFS文件是否目录发生异常,文件路径为:" + fileName, e);
            return false;
        }
    }

    /**
     * 确认hdfs地址是否为目录
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    public boolean isFile(String fileName) {
        try {
            FileStatus fsStatus = fileSystem.getFileStatus(new Path(fileName));
            return fsStatus.isFile();
        } catch (IOException e) {
            LOGGER.error("确认HDFS文件是否文件发生异常,文件路径为:" + fileName, e);
            return false;
        }
    }

    /**
     * 路径下是否有目录
     *
     * @param pathName 路径地址
     * @return
     * @throws Exception
     */
    public boolean existDirectory(String pathName) {
        try {
            fileSystem = getFileSystem();
            Path       path     = new Path(pathName);
            FileStatus fsStatus = fileSystem.getFileStatus(path);
            if (!fsStatus.isDirectory()) {
                return false;
            }
            FileStatus[] fileStatusList = fileSystem.listStatus(path);
            if (null == fileStatusList || 0 == fileStatusList.length) {
                return false;
            }
            for (FileStatus file : fileStatusList) {
                if (file.isDirectory()) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 检查是否空目录
     *
     * @param fileName
     * @return
     */
    public boolean checkEmptyDir(String fileName) {
        try {
            Path path = new Path(fileName);
            if (fileSystem.isDirectory(path)) {
                FileStatus[] fileStatuses = fileSystem.listStatus(path);
                if (fileStatuses.length == 0) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOGGER.error("校验HDFS文件是否空目录发生异常，路径：{}", fileName, e);
        }
        return false;
    }

    public void purgeDirectory(String fileName) {
        try {
            Path path = new Path(fileName);
            fileSystem.delete(path, true);
        } catch (IOException e) {
            LOGGER.error("确认HDFS文件是否目录发生异常,文件路径为:" + fileName, e);
        }
    }

    /**
     * target 不可在directory中
     * @param directory
     * @param targetFile
     */
    public void convergeFile(String directory, String targetFile) {
        Path               directoryPath  = new Path(directory);
        Path               targetFilePath = new Path(targetFile);
        FSDataOutputStream fsOutStream    = null;
        try {
            // the path point to a file
            if (fileSystem.exists(targetFilePath)) {
                fileSystem.delete(targetFilePath, false);
            }
            fsOutStream = fileSystem.create(targetFilePath);
            if (fileSystem.isDirectory(directoryPath)) {
                FileStatus[] sourceFileStatuses = fileSystem.listStatus(directoryPath);
                if (sourceFileStatuses.length != 0) {
                    for (FileStatus sourceFileStatus : sourceFileStatuses) {
                        FSDataInputStream in = null;
                        try {
                            in = fileSystem.open(sourceFileStatus.getPath());
                            IOUtils.copyBytes(in, fsOutStream, 4096, false);
                        } catch (IOException e) {
                            LOGGER.error("将目标文件 {} 合并输出 {} error", sourceFileStatus.getPath().getName(), targetFile, e);
                        } finally {
                            try {
                                if (in != null) {
                                    in.close();
                                }
                            } catch (IOException e) {
                                LOGGER.error("关闭输入流error", e);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("将目标文件夹 {} 合并输出 {} error", directory, targetFile, e);
        } finally {
            try {
                assert fsOutStream != null;
                fsOutStream.close();
            } catch (IOException e) {
                LOGGER.error("关闭输出流error", e);
            }
        }
    }
}