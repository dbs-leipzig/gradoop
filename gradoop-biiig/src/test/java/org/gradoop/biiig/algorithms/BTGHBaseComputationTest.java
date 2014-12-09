package org.gradoop.biiig.algorithms;

import com.google.common.collect.Lists;
import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.gradoop.GConstants;
import org.gradoop.GiraphClusterTest;
import org.gradoop.biiig.io.formats.BTGHBaseVertexInputFormat;
import org.gradoop.biiig.io.formats.BTGHBaseVertexOutputFormat;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.SingleVertexReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryEdge;
import org.gradoop.model.inmemory.MemoryVertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Tests for {@link BTGComputation} that reads BTGs from HBase.
 */
public class BTGHBaseComputationTest extends GiraphClusterTest {
  public BTGHBaseComputationTest() {
    super(BTGHBaseComputationTest.class.getName());
  }

  @Test
  public void testConnectedIIG()
    throws IOException, InterruptedException, ClassNotFoundException {
    // prepare data
    BufferedReader inputReader = createTestReader(BTGComputationTestHelper
      .getConnectedIIG());
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader = new AdjacencyListReader
      (graphStore, new BTGLineReader());
    adjacencyListReader.read(inputReader);

    // run giraph
    compute();
    // test results
    validateConnectedIIG(graphStore);
    // clean up
    graphStore.close();
  }

  private void validateConnectedIIG(GraphStore store) {
    // master data nodes
    validateVertex(store, 0L, 4L, 9L);
    validateVertex(store, 1L, 4L, 9L);
    validateVertex(store, 2L, 4L, 9L);
    validateVertex(store, 3L, 4L, 9L);
    // transactional data nodes btg 1
    validateVertex(store, 4L, 4L);
    validateVertex(store, 5L, 4L);
    validateVertex(store, 6L, 4L);
    validateVertex(store, 7L, 4L);
    validateVertex(store, 8L, 4L);
    // transactional data nodes btg 2
    validateVertex(store, 9L, 9L);
    validateVertex(store, 10L, 9L);
    validateVertex(store, 11L, 9L);
    validateVertex(store, 12L, 9L);
    validateVertex(store, 13L, 9L);
    validateVertex(store, 14L, 9L);
    validateVertex(store, 15L, 9L);
  }

  @Test
  public void testDisconnectedIIG()
    throws IOException, InterruptedException, ClassNotFoundException {
    // prepare data
    BufferedReader inputReader = createTestReader(BTGComputationTestHelper
      .getDisconnectedIIG());
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader = new AdjacencyListReader
      (graphStore, new BTGLineReader());
    adjacencyListReader.read(inputReader);

    // run giraph
    compute();
    // test results
    validateDisconnectedIIG(graphStore);
    // clean up
    graphStore.close();
  }

  private void validateDisconnectedIIG(GraphStore store) {
    // master data nodes btg 1
    validateVertex(store, 0L, 6L);
    validateVertex(store, 1L, 6L);
    validateVertex(store, 2L, 6L);
    // master data nodes btg 2
    validateVertex(store, 3L, 10L);
    validateVertex(store, 4L, 10L);
    validateVertex(store, 5L, 10L);
    // transactional data vertices btg 1
    validateVertex(store, 6L, 6L);
    validateVertex(store, 7L, 6L);
    validateVertex(store, 8L, 6L);
    validateVertex(store, 9L, 6L);
    // transactional data vertices btg 2
    validateVertex(store, 10L, 10L);
    validateVertex(store, 11L, 10L);
    validateVertex(store, 12L, 10L);
    validateVertex(store, 13L, 10L);
  }

  private void validateVertex(final GraphStore store, final Long vertexID,
                              final Long... expectedBTGIds) {
    Vertex v = store.readVertex(vertexID);
    assertNotNull(v);
    assertEquals(expectedBTGIds.length, v.getGraphCount());
    List<Long> expectedGraphs = Lists.newArrayList(expectedBTGIds);
    for (Long g : v.getGraphs()) {
      assertTrue(expectedGraphs.contains(g));
    }
  }

  /**
   * Runs the BTG computation on the local test cluster.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void compute()
    throws IOException, ClassNotFoundException, InterruptedException {
    // setup in- and output tables
    Configuration conf = utility.getConfiguration();
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);

    // setup giraph job
    GiraphJob job = new GiraphJob(conf, BspCase.getCallingMethodName());
    GiraphConfiguration giraphConf = job.getConfiguration();
    setupConfiguration(job);

    giraphConf.setComputationClass(BTGComputation.class);
    giraphConf.setVertexInputFormatClass(BTGHBaseVertexInputFormat.class);
    giraphConf.setVertexOutputFormatClass(BTGHBaseVertexOutputFormat.class);

    assertTrue(job.run(true));
  }

  /**
   * Reads vertices from the sample graphs in
   * {@link org.gradoop.biiig.algorithms.BTGComputationTestHelper}
   */
  private static class BTGLineReader extends SingleVertexReader {
    final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(",");
    final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile(" ");

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex readVertex(String line) {
      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      Long vertexID = Long.valueOf(lineTokens[0]);
      String[] valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
      // read vertex type as label
      String vertexType = valueTokens[0];
      // edges
      String[] stringEdges = VALUE_TOKEN_SEPARATOR.split(lineTokens[2]);
      List<Edge> edges = Lists.newArrayListWithCapacity(stringEdges.length);
      for (String edge : stringEdges) {
        Long otherID = Long.valueOf(edge);
        edges.add(new MemoryEdge(otherID, 0L));
      }
      return new MemoryVertex(vertexID, Lists.newArrayList(vertexType),
        null, edges, null, null);
    }
  }
}
