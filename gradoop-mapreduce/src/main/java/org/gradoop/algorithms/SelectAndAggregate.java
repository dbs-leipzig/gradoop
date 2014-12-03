package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.gradoop.GConstants;
import org.gradoop.io.formats.PairWritable;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryGraph;
import org.gradoop.model.operators.VertexIntegerAggregate;
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
   * Aggregate specific property.
   */
  public static final String AGGREGATION_RESULT_PROPERTY_KEY = "agg_sum";

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
    private VertexIntegerAggregate vertexIntegerAggregate;

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

      Class<? extends VertexIntegerAggregate> aggregateClass = conf.getClass(
        SelectAndAggregate.VERTEX_AGGREGATE_CLASS,
        null,
        VertexIntegerAggregate.class
      );

      try {
        this.vertexHandler = handlerClass.getConstructor().newInstance();
        this.vertexPredicate = predicateClass.getConstructor().newInstance();
        this.vertexIntegerAggregate =
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
      IntWritable aggregate = new IntWritable(vertexIntegerAggregate
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
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      Class<? extends GraphHandler> handlerClass = conf.getClass(
        GConstants.GRAPH_HANDLER_CLASS,
        GConstants.DEFAULT_GRAPH_HANDLER,
        GraphHandler.class
      );

      try {
        this.graphHandler = handlerClass
          .getConstructor().newInstance();
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
      int sum = 0;
      boolean predicate = false;
      // calculate aggregate and check if the graph matches the predicate
      for (PairWritable pair : values) {
        sum = sum + pair.getValue().get();
        if (pair.getPredicateResult().get()) {
          predicate = true;
        }
      }

      if (predicate) {
        Graph g = new MemoryGraph(key.get());
        g.addProperty(AGGREGATION_RESULT_PROPERTY_KEY, sum);
        Put put = new Put(Bytes.toBytes(key.get()));
        put = graphHandler.writeProperties(put, g);
        context.write(null, put);
      }
    }
  }
}
