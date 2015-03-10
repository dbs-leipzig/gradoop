package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.GConstants;
import org.gradoop.GradoopClusterTest;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.model.Graph;
import org.gradoop.model.impl.GraphFactory;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Select graphs based on a given vertex property.
 */
public class SelectionTest extends GradoopClusterTest {
  /**
   * Select graphs that contain vertices that own the following property.
   */
  private static final String TEST_PROPERTY_KEY = "k3";
  private static final String TEST_PROPERTY_VALUE = "v3";
  private static final String PREDICATE_PROPERTY_KEY = "pred";

  @Test
  public void selectionTest()
    throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = utility.getConfiguration();
    GraphStore graphStore = createEmptyGraphStore();
    BufferedReader bufferedReader = createTestReader(EXTENDED_GRAPH);
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, new EPGVertexReader());
    // store the graph
    adjacencyListReader.read(bufferedReader);

    // define MapReduce job
    Job job = Job.getInstance(conf, SelectionTest.class.getName());
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    // map
    TableMapReduceUtil.initTableMapperJob(
      GConstants.DEFAULT_TABLE_VERTICES,
      scan,
      SelectionTableMapper.class,
      LongWritable.class,
      BooleanWritable.class,
      job
    );
    // reduce
    TableMapReduceUtil.initTableReducerJob(
      GConstants.DEFAULT_TABLE_GRAPHS,
      SelectionTableReducer.class,
      job
    );
    job.setNumReduceTasks(1);
    // run MR job
    job.waitForCompletion(true);

    // validate
    validateGraphs(graphStore);

    graphStore.close();
  }

  private void validateGraphs(GraphStore graphStore) {
    validateGraph(graphStore.readGraph(0L), true);
    // TODO: graph 1 can be validated if issue #15 is fixed
    assertNull(graphStore.readGraph(1L));
//    validateGraph(graphStore.readGraph(1L), false);
  }

  private void validateGraph(Graph graph, boolean expectedPredicate) {
    if (expectedPredicate) {
      assertEquals(1, graph.getPropertyCount());
      assertTrue((Boolean) graph.getProperty(PREDICATE_PROPERTY_KEY));
    } else {
      assertEquals(0, graph.getPropertyCount());
    }
  }

  public static class SelectionTableMapper extends TableMapper<LongWritable,
    BooleanWritable> {

    private static VertexHandler VERTEX_HANDLER = new EPGVertexHandler();
    private static BooleanWritable TRUE = new BooleanWritable(true);

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context)
      throws IOException, InterruptedException {
      Map<String, Object> vertexProps = VERTEX_HANDLER.readProperties(value);
      if (vertexProps.containsKey(TEST_PROPERTY_KEY)) {
        if (vertexProps.get(TEST_PROPERTY_KEY).equals(TEST_PROPERTY_VALUE)) {
          for (Long g : VERTEX_HANDLER.readGraphs(value)) {
            context.write(new LongWritable(g), TRUE);
          }
        }
      }
    }
  }

  public static class SelectionTableReducer extends
    TableReducer<LongWritable, BooleanWritable, ImmutableBytesWritable> {

    private static GraphHandler GRAPH_HANDLER = new EPGGraphHandler();

    @Override
    protected void reduce(LongWritable key, Iterable<BooleanWritable> values,
                          Context context)
      throws IOException, InterruptedException {
      boolean predicate = false;
      for (BooleanWritable booleanWritable : values) {
        if (booleanWritable.get()) {
          predicate = true;
        }
      }
      if (predicate) {
        Graph g = GraphFactory.createDefaultGraphWithID(key.get());
        g.addProperty(PREDICATE_PROPERTY_KEY, true);
        Put put = new Put(GRAPH_HANDLER.getRowKey(key.get()));
        put = GRAPH_HANDLER.writeProperties(put, g);
        context.write(null, put);
      }
    }
  }
}
