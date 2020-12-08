package com.wss.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.*;
import java.net.URL;

/**
 * @author 王森森
 * @create 2020/12/6 13:36
 */
public class HdfsUrlObject {

    @Test
    public void opStream() throws IOException {
        //1.注册url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //2.获取hadoop的文件输入流
        InputStream inputStream = new URL("hdfs://hadoop100:9000/wcinput").openStream();
        //3.获取本地文件输出流
        OutputStream outputStream = new FileOutputStream(new File("F:\\aa.txt"));
        //4.复制文件
        IOUtils.copy(inputStream,outputStream);
        //5.关闭流
        outputStream.close();
        inputStream.close();
    }


}
