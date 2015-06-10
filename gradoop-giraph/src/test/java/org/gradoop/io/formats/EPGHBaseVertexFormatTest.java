package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.GiraphClusterTest;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.model.Edge;
import org.gradoop.storage.GraphStore;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Test class for storing and reading an EPG in the default format.
 */
public class EPGHBaseVertexFormatTest extends GiraphClusterTest {
  private static Logger LOG = Logger.getLogger(EPGHBaseVertexFormatTest.class);

  private final static String TEST_LABEL = "test";
  private final static String TEST_KEY = "test_key";
  private final static String TEST_VALUE = "test_value";
  private final static Long TEST_GRAPH = 2L;
  private final static Long TEST_SOURCE_VERTEX = 1L;
  private final static Long TEST_TARGET_VERTEX = 2L;

  public EPGHBaseVertexFormatTest() {
    super(EPGHBaseVertexFormatTest.class.getName());
  }

  @Test
  public void vertexInputOutputTest() throws IOException,
    ClassNotFoundException, InterruptedException {
    BufferedReader bufferedReader = createTestReader(EXTENDED_GRAPH);
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, new EPGVertexReader());
    // store the graph
    adjacencyListReader.read(bufferedReader);

    // setup in- and output tables
    Configuration conf = utility.getConfiguration();
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);

    // setup giraph job
    GiraphJob giraphJob = new GiraphJob(conf, BspCase.getCallingMethodName());
    GiraphConfiguration giraphConfiguration = giraphJob.getConfiguration();
    setupConfiguration(giraphJob);
    giraphConfiguration.setComputationClass(TestComputation.class);
    giraphConfiguration
      .setVertexInputFormatClass(EPGHBaseVertexInputFormat.class);
    giraphConfiguration
      .setVertexOutputFormatClass(EPGHBaseVertexOutputFormat.class);

    assertTrue(giraphJob.run(true));

    // test
    org.gradoop.model.Vertex v = graphStore.readVertex(TEST_SOURCE_VERTEX);

    // label
    assertEquals(TEST_LABEL, v.getLabel());

    // properties
    assertEquals(TEST_VALUE, v.getProperty(TEST_KEY));

    // graphs
    List<Long> graphs = Lists.newArrayList(v.getGraphs());
    assertThat(graphs.size(), is(3));
    assertTrue(graphs.contains(0L));
    assertTrue(graphs.contains(1L));
    assertTrue(graphs.contains(2L));

    // edges
    List<Edge> outEdges = Lists.newArrayList(v.getOutgoingEdges());
    assertThat(outEdges.size(), is(2));
    for (Edge e : outEdges) {
      if (e.getOtherID() == 0L) {
        assertThat(e.getLabel(), is("b"));
        assertThat(e.getIndex(), is(0L));
        assertNotNull(e.getPropertyKeys());
        List<String> propertyKeys = Lists.newArrayList(e.getPropertyKeys());
        assertThat(propertyKeys.size(), is(2));
        for (String k : e.getPropertyKeys()) {
          switch (k) {
          case "k1":
            assertThat(e.getProperty("k1"), Is.<Object>is("v1"));
            break;
          case "k2":
            assertThat(e.getProperty("k2"), Is.<Object>is("v2"));
            break;
          default:
            assertTrue("unexpected property at edge 1L -> 0L", false);
            break;
          }
        }
      } else if (e.getOtherID().equals(TEST_TARGET_VERTEX)) {
        assertThat(e.getLabel(), is("c"));
        assertThat(e.getIndex(), is(1L));
        assertNotNull(e.getPropertyKeys());
        List<String> propertyKeys = Lists.newArrayList(e.getPropertyKeys());
        for (String k : propertyKeys) {
          LOG.info("=== propertyKey: " + k);
        }
        assertThat(propertyKeys.size(), is(1));
        for (String k : propertyKeys) {
          if (k.equals(TEST_KEY)) {
            assertThat(e.getProperty(TEST_KEY), Is.<Object>is(TEST_VALUE));
          } else {
            assertTrue(String
              .format("unexpected property at edge %d -> %d " + "(%s => %s)",
                TEST_SOURCE_VERTEX, TEST_TARGET_VERTEX, k, e.getProperty(k)),
              false);
          }
        }
      } else {
        assertTrue(String
          .format("unexpected outgoing edge %d -> %d", TEST_SOURCE_VERTEX,
            e.getOtherID()), false);
      }
    }

    // close everything
    graphStore.close();
    bufferedReader.close();
  }

  public static class TestComputation extends
    BasicComputation<EPGVertexIdentifierWritable, EPGVertexValueWritable,
      EPGEdgeValueWritable, LongWritable> {

    @Override
    public void compute(
      Vertex<EPGVertexIdentifierWritable, EPGVertexValueWritable,
        EPGEdgeValueWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {
      // modify vertex value
      vertex.getValue().setLabel(TEST_LABEL);
      vertex.getValue().addProperty(TEST_KEY, TEST_VALUE);
      vertex.getValue().addGraph(TEST_GRAPH);

      // modify edge value of edge TEST_SOURCE_VERTEX -> TEST_TARGET_VERTEX
      if (vertex.getId().getID().equals(TEST_SOURCE_VERTEX)) {
        EPGVertexIdentifierWritable vertexIdentifier =
          new EPGVertexIdentifierWritable(TEST_TARGET_VERTEX);

        EPGEdgeValueWritable edgeValue = vertex.getEdgeValue(vertexIdentifier);
        edgeValue.addProperty(TEST_KEY, TEST_VALUE);
        vertex.setEdgeValue(vertexIdentifier, edgeValue);
      }
      vertex.voteToHalt();
    }
  }
}
