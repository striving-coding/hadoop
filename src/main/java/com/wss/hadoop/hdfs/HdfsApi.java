package com.wss.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 王森森
 * @create 2020/12/8 10:36
 */
public class HdfsApi {
    private  Logger logger = LoggerFactory.getLogger(getClass());

    //获取hdfs的抽象封装对象
    static FileSystem fileSystem;
    static {
        try {
            //配置文件  优先级从高到底为 代码中设置》resource》项目中的默认配置
            Configuration configuration = new Configuration();
            configuration.setInt("dfs.replication",1);
            fileSystem = FileSystem.get(new URI("hdfs://hadoop100:9000"), configuration, "hadoop");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void copyToLocalFile() throws IOException {
        //文件下载到本地
        fileSystem.copyToLocalFile(new Path("/ws.docx"),new Path("f:/test.docx"));
    }

    @Test
    public void put() throws IOException {
        //上传文件到hdfs
        for (int i = 0; i < 5; i++) {
            fileSystem.copyFromLocalFile(new Path("f:/11.txt"),new Path("/"+i+".txt"));
        }
    }

    @Test
    public void rename()throws Exception{
        //更名
        fileSystem.rename(new Path("/ws.docx"),new Path("/hdfs.docx"));
    }


    @Test
    public void delete()throws Exception{
        //删除
        boolean delete = fileSystem.delete(new Path("/wss.docx"), true);
        logger.info("删除"+delete);
    }

    @Test
    public void append()throws Exception{
        //追加
        FSDataOutputStream fsDataOutputStream = fileSystem.append(new Path("/22.txt"), 1024);
        FileInputStream fileInputStream = new FileInputStream(new File("f:/11.txt"));
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,1024,true);
    }


    @Test
    public void listStatus()throws Exception{
        //查看文件夹
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            boolean directory = fileStatus.isDirectory();
            if (directory){
                System.out.println("文件路径"+fileStatus.getPath());
            }else {
                System.out.println(fileStatus.getPath());
            }
        }
    }

    @Test
    public void listFiles() throws IOException {
        //查看文件 文件可以查看块 使用getBlockLocations
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath());
            for (BlockLocation blockLocation : fileStatus.getBlockLocations()) {
                for (String host : blockLocation.getHosts()) {
                    System.out.println(host);
                }
            }
        }
    }


    @After
    public void closeFileSystem() throws IOException {
        fileSystem.close();
    }


}
