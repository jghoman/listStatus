package com.mycompany.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for simple App.
 */
public class AppTest {
  @Test
  public void listStatusReturnsFiles() throws IOException, URISyntaxException {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsFileSystem.SCHEME);
      FileSystem fs = webHdfs;

      fs.mkdirs(new Path("/dir1"));
      fs.mkdirs(new Path("/dir2"));
      fs.create(new Path("/f1")).close();
      fs.create(new Path("/f2")).close();

      FileStatus[] ls = fs.listStatus(new Path("/"));
      assertEquals(4, ls.length);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
