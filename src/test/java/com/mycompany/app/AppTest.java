package com.mycompany.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class AppTest {
  @Test
  public void listStatusReturnsFiles() throws IOException, URISyntaxException {
    MiniDFSCluster cluster = null;
    FileSystem webHdfs = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsFileSystem.SCHEME);

      webHdfs.mkdirs(new Path("/dir1"));
      webHdfs.mkdirs(new Path("/dir2"));
      webHdfs.create(new Path("/f1")).close();
      webHdfs.create(new Path("/f2")).close();

      FileStatus[] ls = webHdfs.listStatus(new Path("/"));
      assertEquals(4, ls.length);

    } finally {
      if (webHdfs != null) {
        webHdfs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
