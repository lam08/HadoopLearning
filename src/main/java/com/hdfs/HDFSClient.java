package com.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jiangyu on 2020/12/26 16:12
 */
public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();

        //conf.set("fs.defaultFS","hdfs://hadoop01:9000");


        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root");

        fs.mkdirs(new Path("/user/lam"));


        fs.close();

        System.out.println("end");
    }

}
