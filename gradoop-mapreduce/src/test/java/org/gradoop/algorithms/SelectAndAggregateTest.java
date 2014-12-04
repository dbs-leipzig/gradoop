package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.GConstants;
import org.gradoop.MapReduceClusterTest;
import org.gradoop.io.formats.PairWritable;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.model.operators.VertexDoubleAggregate;
import org.gradoop.model.operators.VertexPredicate;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Simple test for a select and aggregate map reduce job.
 */
public class SelectAndAggregateTest extends MapReduceClusterTest {

  private static final String[] TEST_GRAPH = new String[] {
    "1|A|1 pos 4 1000 type 1 0|||1 1",
    "2|A|1 pos 4 1000 type 1 0|||2 1 2",
    "3|A|1 pos 4 500 type 1 1|||1 1",
    "4|A|2 neg 4 1000 type 1 1|||1 2",
    "5|A|1 neg 4 100 type 1 1|||1 2"
  };

  /**
   * Aggregate specific property.
   */
  public static final String PROJECTION_PROPERTY_KEY1 = "pos";
  /**
   * Aggregate specific property.
   */
  public static final String PROJECTION_PROPERTY_KEY2 = "neg";
  /**
   * Graph property key where the result will be stored.
   */
  private static final String AGG_RESULT_KEY = "agg_sum";
  /**
   * Property key a vertex has to have to fulfil the predicate.
   */
  private static final String PREDICATE_KEY = "type";
  /**
   * Property value a vertex has to have to fulfil the predicate.
   */
  private static final Integer PREDICATE_VALUE = 1;

  @Test
  public void selectAndAggregateTest()
    throws IOException, ClassNotFoundException, InterruptedException {
    // setup
    Configuration conf = utility.getConfiguration();
    GraphStore graphStore = createEmptyGraphStore();
    BufferedReader br = createTestReader(TEST_GRAPH);
    AdjacencyListReader adjacencyListReader = new AdjacencyListReader
      (graphStore, new EPGVertexReader());
    adjacencyListReader.read(br);

    /*
    Setup MapReduce Job SelectAndAggreagte
     */

    // Mapper settings
    conf.setClass(GConstants.VERTEX_HANDLER_CLASS, EPGVertexHandler.class,
      VertexHandler.class);
    conf.setClass(SelectAndAggregate.VERTEX_PREDICATE_CLASS,
      TestPredicate.class,
      VertexPredicate.class);
    conf.setClass(SelectAndAggregate.VERTEX_AGGREGATE_CLASS,
      TestVertexAggregate.class, VertexDoubleAggregate.class);
    // Reducer settings
    conf.setClass(GConstants.GRAPH_HANDLER_CLASS, EPGGraphHandler.class,
      GraphHandler.class);
    conf.set(SelectAndAggregate.AGGREGATE_RESULT_PROPERTY_KEY, AGG_RESULT_KEY);
    conf.setClass(SelectAndAggregate.PAIR_AGGREGATE_CLASS,
      TestPairAggregator.class, PairAggregator.class);
    // vertex predicate for select step


    Job job = new Job(conf, SelectAndAggregateTest.class.getName());
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);

    // map
    TableMapReduceUtil.initTableMapperJob(
      GConstants.DEFAULT_TABLE_VERTICES,
      scan,
      SelectAndAggregate.SelectMapper.class,
      LongWritable.class,
      PairWritable.class,
      job
    );

    // reduce
    TableMapReduceUtil.initTableReducerJob(
      GConstants.DEFAULT_TABLE_GRAPHS,
      SelectAndAggregate.AggregateReducer.class,
      job
    );
    job.setNumReduceTasks(1);
    // run MR job
    job.waitForCompletion(true);

    // validate
    validateGraphs(graphStore);

    // cleanup
    br.close();
    graphStore.close();
  }

  private void validateGraphs(GraphStore graphStore) {
    validateGraph(graphStore.readGraph(1L), 2500.0);
    validateGraph(graphStore.readGraph(2L), -100.0);
  }

  private void validateGraph(Graph g, double expectedValue) {
    assertEquals(1, g.getPropertyCount());
    assertEquals(expectedValue, g.getProperty(AGG_RESULT_KEY));
  }

  /**
   * Returns true if the vertex has the given key-value-pair.
   */
  public static class TestPredicate implements VertexPredicate {

    @Override
    public boolean evaluate(Vertex vertex) {
      boolean result = false;
      if (vertex.getPropertyCount() > 0) {
        Object o = vertex.getProperty(PREDICATE_KEY);
        result = (o != null && o.equals(PREDICATE_VALUE));
      }
      return result;
    }
  }

  /**
   * Domain specific vertex aggregation. Sums up positive and negative
   * property values stored at a vertex.
   */
  public static class TestVertexAggregate implements VertexDoubleAggregate {

    @Override
    public Double aggregate(Vertex vertex) {
      double calcValue = 0;
      if (vertex.getPropertyCount() > 0) {
        Object o = vertex.getProperty(PROJECTION_PROPERTY_KEY1);
        if (o != null) {
          calcValue += (double) o;
        }
        o = vertex.getProperty(PROJECTION_PROPERTY_KEY2);
        if (o != null) {
          calcValue -= (double) o;
        }
      }
      return calcValue;
    }
  }

  /**
   * Domain specific aggregation of the vertex values.
   */
  public static class TestPairAggregator implements PairAggregator {

    @Override
    public Pair<Boolean, Double> aggregate(Iterable<PairWritable> values) {
      double sum = 0f;
      boolean predicate = false;
      for (PairWritable value : values) {
        sum = sum + value.getValue().get();
        if (value.getPredicateResult().get()) {
          predicate = true;
        }
      }
      return new Pair<>(predicate, sum);
    }
  }
}
