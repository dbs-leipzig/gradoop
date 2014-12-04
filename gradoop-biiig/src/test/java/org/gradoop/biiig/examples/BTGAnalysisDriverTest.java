package org.gradoop.biiig.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gradoop.MapReduceClusterTest;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Tests the pipeline described in
 * {@link org.gradoop.biiig.examples.BTGAnalysisDriver}.
 */
public class BTGAnalysisDriverTest extends MapReduceClusterTest {

  @Test
  public void driverTest()
    throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "btg.graph";
    String outputDir = "/output";

    prepareInput(graphFile);

    BTGAnalysisDriver btgAnalysisDriver = new BTGAnalysisDriver();
    btgAnalysisDriver.setConf(conf);

    int exitCode = btgAnalysisDriver.run(new String[] {graphFile, outputDir});

    assertThat(exitCode, is(0));
  }

  private void prepareInput(String inputFile)
    throws IOException {
    URL tmpUrl = Thread.currentThread().getContextClassLoader().getResource
      (inputFile);
    assertNotNull(tmpUrl);
    String graphFileResource = tmpUrl.getPath();
    // copy input graph to DFS
    FileSystem fs = utility.getTestFileSystem();
    Path graphFileLocalPath = new Path(graphFileResource);
    Path graphFileDFSPath = new Path(inputFile);
    fs.copyFromLocalFile(graphFileLocalPath, graphFileDFSPath);
  }
}
