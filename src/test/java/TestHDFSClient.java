import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jiangyu on 2020/12/26 19:33
 */
public class TestHDFSClient {

    FileSystem fs;
    Configuration conf;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
    }

    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(new Path("3.txt"), new Path("/user/test"));
    }

    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(new Path("/user/test/1.txt"), new Path("4.txt"));
    }

    @Test
    public void testDelete() throws IOException {
        fs.delete(new Path("/user/test1"), false);
    }

    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/user/test/3.txt"), new Path("/user/test/4.txt"));
    }

    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/user/test"), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());

            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
                System.out.println("==============");
            }
        }
    }

    @Test
    public void testListStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user"));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }

    }

    @Test
    public void testPut() throws IOException {
        FileInputStream fis = new FileInputStream(new File("3.txt"));
        FSDataOutputStream fos = fs.create(new Path("/user/test/5.txt"));
        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    @Test
    public void tesGet() throws IOException {
        FSDataInputStream fis = fs.open(new Path("/user/test/5.txt"));
        FileOutputStream fos = new FileOutputStream(new File("5.txt"));
        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }



    @After
    public void after() throws IOException {
        fs.close();
    }

}
