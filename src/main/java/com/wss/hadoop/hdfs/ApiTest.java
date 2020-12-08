package com.wss.hadoop.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 王森森
 * @create 2020/12/6 14:55
 */
public class ApiTest {
    private static FileSystem fileSystem;

    static {
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop100:9000"),new Configuration(),"hadoop");
        } catch (IOException e) {

            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //递归查询所有文件
    @Test
    public void eachFile() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath()+"**************"+ fileStatus.getPath().getName());
            System.out.println("block数"+fileStatus.getBlockLocations().length);
        }

        fileSystem.close();
    }


    //创建文件夹  可以递归创建
    @Test
    public void mkdir()throws Exception{
        boolean mkdirs = fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        System.out.println(mkdirs);
        fileSystem.close();
    }

    //创建文件  文件目录不存在可以递归创建文件目录
    @Test
    public void creatFile()throws Exception{
        fileSystem.create(new Path("/aaa/bbb/ccc/test.txt"));
        fileSystem.close();
    }


    //上传文件
    @Test
    public void upLoadFile()throws Exception{
        fileSystem.copyFromLocalFile(new Path("F:/nuli.txt"),new Path("/aini.txt"));
        fileSystem.close();
    }

    //下载文件  方式一
    @Test
    public void downLoadFileOne()throws Exception{
        //1.获取hdfs的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/wcinput"));
        //2.获取本地的输出流
            FileOutputStream outputStream = new FileOutputStream(new File("F:/wwww.txt"));
        //3.拷贝文件
        IOUtils.copy(inputStream,outputStream);
        //4.关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    //下载文件 方式二
    @Test
    public void downLoadFileTwo()throws Exception{
        //1.调用方法，实现文件的下载
        fileSystem.copyToLocalFile(new Path("/aini.txt"),new Path("F:/wsssssss.txt"));
        //2关闭流
        fileSystem.close();
    }


    //合并小文件上传到hdfs
    @Test
    public void mergeFile() throws Exception{
        //1.获取分布式文件系统
        FSDataOutputStream outputStream = fileSystem.create(new Path("/getmerge"));
        //2.获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //3.获取本地文件系统目录下的所有文件集合
        FileStatus[] fileStatuses = local.listStatus(new Path("file:///F:\\testxml"));
        //4.拷贝文件
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream,outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(local);
        fileSystem.close();
    }


}
