package org.gradoop.algorithms;

import com.google.common.collect.Lists;
import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.gradoop.GConstants;
import org.gradoop.GiraphClusterTest;
import org.gradoop.io.formats.EPGHBaseVertexInputFormat;
import org.gradoop.io.formats.SubgraphExtractionVertexOutputFormat;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * CC Test
 */
public class ConnectedComponentsComputationTest extends GiraphClusterTest {

  public ConnectedComponentsComputationTest() {
    super(ConnectedComponentsComputationTest.class.getName());
  }

  // if graphs are already assigned, reset and new graph assignment fails
  private static final String[] MY_EXTENDED_GRAPH = new String[]{
    "0|A|3 k1 5 v1 k2 5 v2 k3 5 v3|a.1.0 1 k1 5 v1|b.1.0 2 k1 5 v1 k2 5 v2|0",
    "1|A B|2 k1 5 v1 k2 5 v2|b.0.0 2 k1 5 v1 k2 5 v2," +
      "c.2.1 0|a.0.0 1 k1 5 v1|0",
    "2|C|2 k1 5 v1 k2 5 v2|d.2.0 0|d.2.0 0,c.1.1 0|0"
  };

  @Test
  public void extendedGraphVertexIOTest() throws InterruptedException,
    IOException, ClassNotFoundException {
    EPGVertexReader reader = new EPGVertexReader();
    vertexInputOutputTest(MY_EXTENDED_GRAPH, reader);
  }

  public void vertexInputOutputTest(String[] input, VertexLineReader reader)
    throws IOException, ClassNotFoundException, InterruptedException {
    BufferedReader bufferedReader = createTestReader(input);
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, reader);
    // store the graph
    adjacencyListReader.read(bufferedReader);

    //pre-computation test, graphs should be 0
    List<Long> preGraphs =
      Lists.newArrayList(graphStore.readVertex(0L).getGraphs());
    assertThat(preGraphs.size(), is(0));

    // setup in- and output tables
    Configuration conf = utility.getConfiguration();
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);

    // setup giraph job
    GiraphJob giraphJob = new GiraphJob(conf, BspCase.getCallingMethodName());
    GiraphConfiguration giraphConf = giraphJob.getConfiguration();
    setupConfiguration(giraphJob);

    giraphConf.setComputationClass(ConnectedComponentsComputation.class);
    giraphConf.setVertexInputFormatClass(EPGHBaseVertexInputFormat.class);
    giraphConf.setBoolean(EPGHBaseVertexInputFormat.READ_INCOMING_EDGES, true);
    giraphConf.setVertexOutputFormatClass(
      SubgraphExtractionVertexOutputFormat.class);

    assertTrue(giraphJob.run(true));

    List<org.gradoop.model.Vertex> vertexList = Lists.newArrayList();

    //post-computation test, graphs should be 1
    vertexList.add(graphStore.readVertex(0L));
    vertexList.add(graphStore.readVertex(1L));
    vertexList.add(graphStore.readVertex(2L));
    for (org.gradoop.model.Vertex v : vertexList) {
      List<Long> graphs = Lists.newArrayList(v.getGraphs());
      assertThat(graphs.size(), is(1));
      assertThat(graphs.contains(0L), is(true));
    }

    graphStore.close();
    bufferedReader.close();
  }
}
