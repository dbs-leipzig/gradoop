package org.gradoop.algorithms;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.gradoop.GConstants;
import org.gradoop.GiraphClusterTest;
import org.gradoop.io.formats.EPGIdWithValueVertexOutputFormat;
import org.gradoop.io.formats.EPGLongLongNullVertexInputFormat;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.SingleVertexReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Tests for {@link org.gradoop.algorithms.LabelPropagationComputation}
 */
public class HBaseLabelPropagationComputationTest extends GiraphClusterTest {
  public HBaseLabelPropagationComputationTest() {
    super(HBaseLabelPropagationComputationTest.class.getName());
  }

  @Test
  public void testConnectedGraph() throws IOException, InterruptedException,
    ClassNotFoundException {
    // prepare data
    BufferedReader inputReader = createTestReader(
      GiraphTestHelper.getConnectedGraphWithVertexValues());
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, new PartitioningLineReader());
    adjacencyListReader.read(inputReader);
    // run giraph
    compute();
    // test results
    validateConnectedGraph(graphStore);
    // clean up
    graphStore.close();
  }

  private void validateConnectedGraph(GraphStore store) {
    validateVertex(store, 0, 0);
    validateVertex(store, 1, 0);
    validateVertex(store, 2, 0);
    validateVertex(store, 3, 0);
    validateVertex(store, 4, 4);
    validateVertex(store, 5, 4);
    validateVertex(store, 6, 4);
    validateVertex(store, 7, 4);
  }

  @Test
  public void testBipartiteGraph() throws IOException, InterruptedException,
    ClassNotFoundException {
    // prepare data
    BufferedReader inputReader = createTestReader(
      GiraphTestHelper
        .getCompleteBipartiteGraphWithVertexValue());
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, new PartitioningLineReader());
    adjacencyListReader.read(inputReader);
    // run giraph
    compute();
    // test results
    validateBipartiteGraph(graphStore);
    // clean up
    graphStore.close();
  }

  private void validateBipartiteGraph(GraphStore store) {
    validateVertex(store, 0, 0);
    validateVertex(store, 1, 0);
    validateVertex(store, 2, 0);
    validateVertex(store, 3, 0);
    validateVertex(store, 4, 0);
    validateVertex(store, 5, 0);
    validateVertex(store, 6, 0);
    validateVertex(store, 7, 0);
  }


  private void validateVertex(final GraphStore store, final long vertexID,
    final long expectedValue) {
    Vertex v = store.readVertex(vertexID);
    assertNotNull(v);
    assertEquals(1, v.getPropertyCount());
    assertEquals(expectedValue,
      v.getProperty(EPGLongLongNullVertexInputFormat.VALUE_PROPERTY_KEY));
  }

  private void compute() throws IOException, ClassNotFoundException,
    InterruptedException {
    // setup in- and output tables
    Configuration conf = utility.getConfiguration();
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    // setup giraph job
    GiraphJob job = new GiraphJob(conf, BspCase.getCallingMethodName());
    GiraphConfiguration giraphConf = job.getConfiguration();
    setupConfiguration(job);
    giraphConf.setComputationClass(LabelPropagationComputation.class);
    giraphConf
      .setVertexInputFormatClass(EPGLongLongNullVertexInputFormat.class);
    giraphConf
      .setVertexOutputFormatClass(EPGIdWithValueVertexOutputFormat.class);
    assertTrue(job.run(true));
  }

  private static class PartitioningLineReader extends SingleVertexReader {
    final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex readVertex(String line) {
      final String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      // read vertex id
      long vertexID = Long.valueOf(lineTokens[0]);
      // read vertex value
      Map<String, Object> properties = Maps.newHashMapWithExpectedSize(1);
      properties.put(EPGLongLongNullVertexInputFormat.VALUE_PROPERTY_KEY,
        Long.valueOf(lineTokens[1]));
      List<Edge> edges = Lists.newArrayListWithCapacity(lineTokens.length - 2);
      for (int n = 2; n < lineTokens.length; n++) {
        long otherID = Long.valueOf(lineTokens[n]);
        edges.add(EdgeFactory.createDefaultEdge(otherID, (long) n - 2));
      }
      return VertexFactory
        .createDefaultVertexWithProperties(vertexID, properties, edges);
    }
  }
}

