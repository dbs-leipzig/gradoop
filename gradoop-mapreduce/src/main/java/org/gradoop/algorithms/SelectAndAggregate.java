package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.gradoop.GConstants;
import org.gradoop.io.formats.PairWritable;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.MemoryGraph;
import org.gradoop.model.operators.VertexDoubleAggregate;
import org.gradoop.model.operators.VertexPredicate;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Very static and POC MapReduce algorithm for the selection of graphs based
 * on a vertex predicate and the projection of defined attributes for
 * aggregation in a reduce step.
 */
public class SelectAndAggregate {
  /**
   * True
   */
  private static final BooleanWritable TRUE = new BooleanWritable(true);
  /**
   * False
   */
  private static final BooleanWritable FALSE = new BooleanWritable(false);
  /**
   * Configuration key for the predicate class used in the selection step.
   */
  public static final String VERTEX_PREDICATE_CLASS = SelectAndAggregate
    .class.getName() + ".predicate";
  /**
   * Configuration key for the vertex aggregate class used in the selection
   * step.
   */
  public static final String VERTEX_AGGREGATE_CLASS = SelectAndAggregate
    .class.getName() + ".aggregate";
  /**
   * Class to be used to calculate the final result for a single graph in the
   * reduce phase.
   */
  public static final String PAIR_AGGREGATE_CLASS = SelectAndAggregate
    .class.getName() + ".aggregate.class";
  /**
   * Graph property key to store the aggregation result at.
   */
  public static final String AGGREGATE_RESULT_PROPERTY_KEY =
    SelectAndAggregate.class.getName() + ".aggregate_result_property_key";

  public static final String DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY =
    "agg_result";

  /**
   * In the map phase, graphs are selected based on a given vertex attribute.
   */
  public static class SelectMapper extends TableMapper<LongWritable,
    PairWritable> {

    /**
     * Reads/writes vertices from/to HBase.
     */
    private VertexHandler vertexHandler;
    /**
     * Used to evaluate the vertex.
     */
    private VertexPredicate vertexPredicate;
    /**
     * Used to calculate a vertex aggregate.
     */
    private VertexDoubleAggregate vertexDoubleAggregate;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      Class<? extends VertexHandler> handlerClass = conf.getClass(
        GConstants.VERTEX_HANDLER_CLASS,
        GConstants.DEFAULT_VERTEX_HANDLER,
        VertexHandler.class
      );

      Class<? extends VertexPredicate> predicateClass = conf.getClass(
        SelectAndAggregate.VERTEX_PREDICATE_CLASS,
        null,
        VertexPredicate.class
      );

      Class<? extends VertexDoubleAggregate> aggregateClass = conf.getClass(
        SelectAndAggregate.VERTEX_AGGREGATE_CLASS,
        null,
        VertexDoubleAggregate.class
      );

      try {
        this.vertexHandler = handlerClass.getConstructor().newInstance();
        this.vertexPredicate = predicateClass.getConstructor().newInstance();
        this.vertexDoubleAggregate =
          aggregateClass.getConstructor().newInstance();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context)
      throws IOException, InterruptedException {
      Vertex v = vertexHandler.readVertex(value);

      boolean predicate = vertexPredicate.evaluate(v);
      DoubleWritable aggregate = new DoubleWritable(vertexDoubleAggregate
        .aggregate(v));

      for (Long graph : vertexHandler.readGraphs(value)) {
        if (predicate) {
          context.write(new LongWritable(graph), new PairWritable(TRUE,
            aggregate));
        } else {
          context.write(new LongWritable(graph), new PairWritable(FALSE,
            aggregate));
        }
      }
    }
  }

  /**
   * Checks all vertices of a graph for the predicate result and aggregates the
   * values.
   */
  public static class AggregateReducer extends TableReducer<LongWritable,
    PairWritable, ImmutableBytesWritable> {
    /**
     * Reads/writes graph from/to HBase.
     */
    private GraphHandler graphHandler;

    /**
     * Used to aggregate the values coming from the map phase.
     */
    private PairAggregator pairAggregator;

    /**
     * Graph property key where the aggregated value will be stored.
     */
    private String aggregateResultPropertyKey;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      this.aggregateResultPropertyKey = conf.get(
        SelectAndAggregate.AGGREGATE_RESULT_PROPERTY_KEY,
        SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY);

      Class<? extends GraphHandler> handlerClass = conf.getClass(
        GConstants.GRAPH_HANDLER_CLASS,
        GConstants.DEFAULT_GRAPH_HANDLER,
        GraphHandler.class
      );

      Class<? extends PairAggregator> aggregatorClass = conf.getClass(
        SelectAndAggregate.PAIR_AGGREGATE_CLASS,
        null,
        PairAggregator.class
      );

      try {
        this.graphHandler = handlerClass.getConstructor().newInstance();
        this.pairAggregator = aggregatorClass.getConstructor().newInstance();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reduce(LongWritable key, Iterable<PairWritable> values,
                          Context context)
      throws IOException, InterruptedException {
      // compute the result for the graph
      Pair<Boolean, Double> result = this.pairAggregator.aggregate(values);
      // if selection predicate evaluates to true, store the aggregate value
      if (result.getFirst()) {
        long graphID = key.get();
        Graph g = new MemoryGraph(graphID);
        g.addProperty(aggregateResultPropertyKey, result.getSecond());
        Put put = new Put(graphHandler.getRowKey(graphID));
        put = graphHandler.writeProperties(put, g);
        context.write(null, put);
      }
    }
  }
}
