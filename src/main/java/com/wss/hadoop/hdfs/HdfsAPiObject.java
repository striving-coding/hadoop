package com.wss.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 王森森
 * @create 2020/12/6 14:20
 */
public class HdfsAPiObject {


    @Test
    //方式一
    public void getFileSystemOne() throws IOException {
        //1.创建configration对象
        Configuration configuration = new Configuration();
        //2.设置文件系统类型   参数1  设置文件系统类型  参数二  设置分布式文件系统
        //file：/// 为本地文件系统 默认为这个不设置的话
        configuration.set("fs.defaultFS","hdfs://hadoop100:9000");
        //3.获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        //4.输出
        System.out.println(fileSystem);
    }

    @Test
    //2.方式二
    public void getFileSystemTwo() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop100:9000"), new Configuration());
        System.out.println(fileSystem);
    }

    @Test
    //3.方式三
    public void getFileSystemThree() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://hadoop100:9000");

        FileSystem fileSystem = FileSystem.newInstance(configuration);

        System.out.println(fileSystem);
    }

    @Test
    //4.方式四
    public void getfilSystemFour() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop100:9000"), new Configuration());

        System.out.println(fileSystem);
    }

}
