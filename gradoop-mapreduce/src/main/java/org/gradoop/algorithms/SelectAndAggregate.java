package org.gradoop.algorithms;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.gradoop.GConstants;
import org.gradoop.io.formats.GenericPairWritable;
import org.gradoop.io.formats.ValueWritable;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.GraphFactory;
import org.gradoop.model.operators.VertexAggregate;
import org.gradoop.model.operators.VertexPredicate;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Very static and POC MapReduce algorithm for the selection of graphs based
 * on a vertex predicate and the projection of defined attributes for
 * aggregation in a reduce step.
 */
public class SelectAndAggregate {
  /**
   * Configuration key for the predicate class used in the selection step.
   */
  public static final String VERTEX_PREDICATE_CLASS =
    SelectAndAggregate.class.getName() + ".predicate";
  /**
   * Configuration key for the vertex aggregate class used in the selection
   * step.
   */
  public static final String VERTEX_AGGREGATE_CLASS =
    SelectAndAggregate.class.getName() + ".aggregate";
  /**
   * Class to be used to calculate the final result for a single graph in the
   * reduce phase.
   */
  public static final String PAIR_AGGREGATE_CLASS =
    SelectAndAggregate.class.getName() + ".aggregate.class";
  /**
   * Graph property key to store the aggregation result at.
   */
  public static final String AGGREGATE_RESULT_PROPERTY_KEY =
    SelectAndAggregate.class.getName() + ".aggregate_result_property_key";
  /**
   * Default graph property key to store aggregation result at.
   */
  public static final String DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY =
    "agg_result";

  /**
   * True
   */
  private static final BooleanWritable TRUE = new BooleanWritable(true);
  /**
   * False
   */
  private static final BooleanWritable FALSE = new BooleanWritable(false);

  /**
   * In the map phase, graphs are selected based on a given vertex attribute.
   */
  public static class SelectMapper extends
    TableMapper<LongWritable, GenericPairWritable> {

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
    private VertexAggregate vertexDoubleAggregate;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {
      Configuration conf = context.getConfiguration();

      Class<? extends VertexHandler> handlerClass = conf
        .getClass(GConstants.VERTEX_HANDLER_CLASS,
          GConstants.DEFAULT_VERTEX_HANDLER, VertexHandler.class);

      Class<? extends VertexPredicate> predicateClass = conf
        .getClass(SelectAndAggregate.VERTEX_PREDICATE_CLASS, null,
          VertexPredicate.class);

      Class<? extends VertexAggregate> aggregateClass = conf
        .getClass(SelectAndAggregate.VERTEX_AGGREGATE_CLASS, null,
          VertexAggregate.class);

      try {
        this.vertexHandler = handlerClass.getConstructor().newInstance();
        this.vertexPredicate = predicateClass.getConstructor().newInstance();
        this.vertexDoubleAggregate =
          aggregateClass.getConstructor().newInstance();
      } catch (NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
      Context context) throws IOException, InterruptedException {
      Vertex v = vertexHandler.readVertex(value);
      LongWritable vertexID = new LongWritable(v.getID());
      boolean predicate = vertexPredicate.evaluate(v);
      ValueWritable aggregate = new ValueWritable();
      aggregate.set(vertexDoubleAggregate.aggregate(v));

      for (Long graph : vertexHandler.readGraphs(value)) {
        if (predicate) {
          context.write(new LongWritable(graph),
            new GenericPairWritable(TRUE, vertexID, aggregate));
        } else {
          context.write(new LongWritable(graph),
            new GenericPairWritable(FALSE, vertexID, aggregate));
        }
      }
    }
  }

  /**
   * Checks all vertices of a graph for the predicate result and aggregates the
   * values.
   */
  public static class AggregateReducer extends
    TableReducer<LongWritable, GenericPairWritable, ImmutableBytesWritable> {
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
    protected void setup(Context context) throws IOException,
      InterruptedException {
      Configuration conf = context.getConfiguration();

      this.aggregateResultPropertyKey = conf
        .get(SelectAndAggregate.AGGREGATE_RESULT_PROPERTY_KEY,
          SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY);

      Class<? extends GraphHandler> handlerClass = conf
        .getClass(GConstants.GRAPH_HANDLER_CLASS,
          GConstants.DEFAULT_GRAPH_HANDLER, GraphHandler.class);

      Class<? extends PairAggregator> aggregatorClass = conf
        .getClass(SelectAndAggregate.PAIR_AGGREGATE_CLASS, null,
          PairAggregator.class);

      try {
        this.graphHandler = handlerClass.getConstructor().newInstance();
        this.pairAggregator = aggregatorClass.getConstructor().newInstance();
      } catch (NoSuchMethodException | InstantiationException |
        InvocationTargetException | IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reduce(LongWritable key,
      Iterable<GenericPairWritable> values, Context context) throws IOException,
      InterruptedException {

      long graphID = key.get();
      Graph g = GraphFactory.createDefaultGraphWithID(graphID);

      // need to cache the pairs, because can only iterate once
      // TODO: workaround for BTGAnalysisDriver. not generic
      List<GenericPairWritable> pairs = Lists.newArrayList();
      // store vertices contained in that graph
      for (GenericPairWritable value : values) {
        g.addVertex(value.getVertexID().get());
        pairs.add(new GenericPairWritable(
          new BooleanWritable(value.getPredicateResult().get()),
          new LongWritable(value.getVertexID().get()),
          new ValueWritable(value.getValue().get())));
      }
      // compute the result for the graph
      Pair<Boolean, ? extends Number> result =
        this.pairAggregator.aggregate(pairs);

      // graph fulfils predicate or has vertices
      if (result.getFirst() || g.getVertexCount() > 0) {
        Put put = new Put(graphHandler.getRowKey(graphID));

        // write vertices to put
        if (g.getVertexCount() > 0) {
          put = graphHandler.writeVertices(put, g);
        }

        // if selection predicate evaluates to true, store the aggregate value
        if (result.getFirst()) {
          g.addProperty(aggregateResultPropertyKey, result.getSecond());
          put = graphHandler.writeProperties(put, g);
        }

        context.write(null, put);
      }
    }
  }
}
