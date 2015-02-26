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
//  private static Logger LOG =
//    Logger.getLogger(ConnectedComponentsComputationTest.class);

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

  // RDF IDs - not working for now
//  private static final Long LGD_ID = 7821830309733859244L;
//  private static final Long GEO_ID = 7282190339445886145L;
//  private static final Long DBP_ID = 7459480252541644492L;
//  private static final Long HEDBP_ID = -7523405104137954346L;
//  private static final Long FOO_ID = 6821830309733859244L;
//
//  // vertex with no column cannot be inserted in HBase
////  private static final String[] NOT_WORKING = new String[]{
////    HEDBP_ID.toString(),
////    GEO_ID.toString() + " " + HEDBP_ID.toString()
////  };
//
//  private static final String[] NEXT_GRAPH = new String[]{
//    LGD_ID.toString() + " " + GEO_ID.toString(),
//    GEO_ID.toString() + " " + HEDBP_ID.toString(),
//    HEDBP_ID.toString() + " " + HEDBP_ID.toString()
//  };
//
//  private static final String[] NEXT_UNDIR_GRAPH = new String[]{
//    LGD_ID.toString() + " " + GEO_ID.toString(),
//    GEO_ID.toString() + " " + HEDBP_ID.toString(),
//    HEDBP_ID.toString() + " " + GEO_ID.toString(),
//    GEO_ID.toString() + " " + LGD_ID
//  };

  @Test
  public void extendedGraphVertexIOTest() throws InterruptedException,
    IOException, ClassNotFoundException {
    EPGVertexReader reader = new EPGVertexReader();
    vertexInputOutputTest(MY_EXTENDED_GRAPH, reader);
  }

  // TODO not working for now, SimpleVertexReader does not write Inc Edges
//  @Test
//  public void simpleVertexIOTest() throws InterruptedException,
//    IOException, ClassNotFoundException {
//    SimpleVertexReader reader = new SimpleVertexReader();
//    vertexInputOutputTest(NEXT_GRAPH, reader);
//  }

  public void vertexInputOutputTest(String[] input, VertexLineReader reader)
    throws IOException, ClassNotFoundException, InterruptedException {
    BufferedReader bufferedReader = createTestReader(input);
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, reader);
    // store the graph
    adjacencyListReader.read(bufferedReader);

    //pre-computation test, graphs should be 0
    if (reader.getClass() == EPGVertexReader.class) {
      List<Long> graphs =
        Lists.newArrayList(graphStore.readVertex(0L).getGraphs());
      assertThat(graphs.size(), is(0));
    }

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
    if (reader.getClass() == EPGVertexReader.class) {
      vertexList.add(graphStore.readVertex(0L));
      vertexList.add(graphStore.readVertex(1L));
      vertexList.add(graphStore.readVertex(2L));
      for (org.gradoop.model.Vertex v : vertexList) {
        List<Long> graphs = Lists.newArrayList(v.getGraphs());
        assertThat(graphs.size(), is(1));
        assertThat(graphs.contains(0L), is(true));
      }
    }

//    if (reader.getClass() == SimpleVertexReader.class) {
//      // TODO better testing
//      vertexList.add(graphStore.readVertex(GEO_ID));
//      vertexList.add(graphStore.readVertex(LGD_ID));
//      vertexList.add(graphStore.readVertex(HEDBP_ID));
//
//      for (org.gradoop.model.Vertex v : vertexList) {
//        LOG.info("=== vertex: " + v.getID().toString());
//        LOG.info("=== vertex: " + v.toString());
//      }
//
//      for (org.gradoop.model.Vertex v : vertexList) {
//        List<Long> graphs = Lists.newArrayList(v.getGraphs());
//        assertThat(graphs.size(), is(1));
//        assertThat(graphs.contains(HEDBP_ID), is(true));
//      }
//    }

    graphStore.close();
    bufferedReader.close();
  }

//  public static class TestComputation extends
//    BasicComputation<EPGVertexIdentifierWritable, EPGVertexValueWritable,
//    EPGEdgeValueWritable, LongWritable> {
//    @Override
//    public void compute(Vertex<EPGVertexIdentifierWritable,
//      EPGVertexValueWritable, EPGEdgeValueWritable> vertex,
//      Iterable<LongWritable> messages) throws IOException {
//      // do nothing
//
//      vertex.voteToHalt();
//    }
//  }
}
