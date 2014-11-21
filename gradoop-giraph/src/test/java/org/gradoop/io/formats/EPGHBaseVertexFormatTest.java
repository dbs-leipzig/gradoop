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
import org.gradoop.GConstants;
import org.gradoop.GiraphClusterBasedTest;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by martin on 20.11.14.
 */
public class EPGHBaseVertexFormatTest extends GiraphClusterBasedTest {
  private final static String TEST_LABEL = "test";
  private final static String TEST_KEY = "test_key";
  private final static String TEST_VALUE = "test_value";

  public EPGHBaseVertexFormatTest() {
    super(EPGHBaseVertexFormatTest.class.getName());
  }

  @Test
  public void vertexInputOutputTest()
    throws IOException, ClassNotFoundException, InterruptedException {
    BufferedReader bufferedReader = createTestReader(EXTENDED_GRAPH);
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, new EPGVertexReader());
    // store the graph
    adjacencyListReader.read(bufferedReader);

    // setup in- and output tables
    Configuration conf = utility.getConfiguration();
    conf.set(TableInputFormat.INPUT_TABLE,
      GConstants.TABLE_VERTICES);
    conf.set(TableOutputFormat.OUTPUT_TABLE,
      GConstants.TABLE_VERTICES);

    // setup giraph job
    GiraphJob giraphJob = new GiraphJob(conf, BspCase.getCallingMethodName());
    GiraphConfiguration giraphConfiguration = giraphJob.getConfiguration();
    setupConfiguration(giraphJob);
    giraphConfiguration.setComputationClass(TestComputation.class);
    giraphConfiguration.setVertexInputFormatClass(EPGHBaseVertexInputFormat
      .class);
    giraphConfiguration.setVertexOutputFormatClass(EPGHBaseVertexOutputFormat
      .class);

    assertTrue(giraphJob.run(true));

    // test
    org.gradoop.model.Vertex v = graphStore.readVertex(1L);
    List<String> labels = Lists.newArrayList(v.getLabels());

    // labels
    assertThat(labels.size(), is(3));
    assertThat(labels.contains("A"), is(true));
    assertThat(labels.contains("B"), is(true));
    assertThat(labels.contains(TEST_LABEL), is(true));

    // properties
    assertEquals(TEST_VALUE, v.getProperty(TEST_KEY));

    // close everything
    graphStore.close();
    bufferedReader.close();
  }

  public static class TestComputation extends
    BasicComputation<EPGVertexIdentifier, EPGLabeledAttributedWritable,
      EPGLabeledAttributedWritable, LongWritable> {

    @Override
    public void compute(
      Vertex<EPGVertexIdentifier, EPGLabeledAttributedWritable,
        EPGLabeledAttributedWritable> vertex,
      Iterable<LongWritable> messages)
      throws IOException {
      vertex.getValue().addLabel(TEST_LABEL);
      vertex.getValue().addProperty(TEST_KEY, TEST_VALUE);
      vertex.voteToHalt();
    }
  }
}
