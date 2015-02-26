package org.gradoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import static org.junit.Assert.assertNotNull;

/**
 * Used for test cases that need a HDFS/HBase/MR mini cluster to run.
 * Initializes a test cluster before the first test runs.
 */
public abstract class GradoopClusterTest extends GradoopTest {

  protected static HBaseTestingUtility utility;

  protected GraphStore createEmptyGraphStore() {
    Configuration config = utility.getConfiguration();
    VertexHandler verticesHandler = new EPGVertexHandler();
    GraphHandler graphsHandler = new EPGGraphHandler();

    HBaseGraphStoreFactory.deleteGraphStore(config);
    return HBaseGraphStoreFactory
      .createGraphStore(config, verticesHandler, graphsHandler);
  }

  protected GraphStore openGraphStore() {
    Configuration config = utility.getConfiguration();
    VertexHandler verticesHandler = new EPGVertexHandler();
    GraphHandler graphsHandler = new EPGGraphHandler();
    return HBaseGraphStoreFactory
      .createGraphStore(config, verticesHandler, graphsHandler);
  }

  /**
   * Copies the given local file into HDFS.
   *
   * @param inputFile path to local file
   * @throws IOException
   */
  protected void copyFromLocal(String inputFile) throws IOException {
    URL tmpUrl =
      Thread.currentThread().getContextClassLoader().getResource(inputFile);
    assertNotNull(tmpUrl);
    String graphFileResource = tmpUrl.getPath();
    // copy input graph to DFS
    FileSystem fs = utility.getTestFileSystem();
    Path graphFileLocalPath = new Path(graphFileResource);
    Path graphFileDFSPath = new Path(inputFile);
    fs.copyFromLocalFile(graphFileLocalPath, graphFileDFSPath);
  }

  /**
   * Starts the mini cluster once for all test cases implementing this class.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility();
      utility.startMiniCluster().waitForActiveAndReadyMaster();
      utility.startMiniMapReduceCluster();
    }
  }

  /**
   * Reads a graph file in HDFS line by line into an array and returns it.
   *
   * @param graphFileName file in HDFS
   * @param lineCount     number of lines
   * @return array with line contents
   * @throws IOException
   */
  protected String[] readGraphFromFile(final Path graphFileName,
    final int lineCount) throws IOException {
    BufferedReader br = new BufferedReader(
      new InputStreamReader(utility.getTestFileSystem().open(graphFileName)));
    String line;
    int i = 0;
    String[] fileContent = new String[lineCount];
    while ((line = br.readLine()) != null) {
      fileContent[i] = line;
      i++;
    }
    return fileContent;
  }
}
