package org.gradoop.io.formats;

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

import static org.junit.Assert.assertTrue;

/**
 * Created by martin on 20.11.14.
 */
public class EPGHBaseVertexFormatTest extends GiraphClusterBasedTest {
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
      vertex.voteToHalt();
    }
  }
}
