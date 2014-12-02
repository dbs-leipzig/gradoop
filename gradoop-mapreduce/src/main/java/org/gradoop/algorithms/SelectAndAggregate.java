package org.gradoop.algorithms;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.gradoop.io.formats.PairWritable;
import org.gradoop.model.Graph;
import org.gradoop.model.inmemory.MemoryGraph;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.Map;

/**
 * Created by martin on 02.12.14.
 */
public class SelectAndAggregate {

  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();
  private static final GraphHandler GRAPH_HANDLER = new EPGGraphHandler();

  private static final BooleanWritable TRUE = new BooleanWritable(true);
  private static final BooleanWritable FALSE = new BooleanWritable(false);

  public static final String PREDICATE_PROPERTY_KEY = "type";
  public static final Integer PREDICATE_PROPERTY_VALUE = 1;

  public static final String PROJECTION_PROPERTY_KEY1 = "pos";
  public static final String PROJECTION_PROPERTY_KEY2 = "neg";

  public static final String AGGREGATION_RESULT_PROPERTY_KEY = "agg_sum";

//  private interface VertexPredicate {
//    boolean evaluate(Vertex v);
//  }
//
//  private static VertexPredicate PREDICATE = new VertexPredicate() {
//    @Override
//    public boolean evaluate(Vertex v) {
//      boolean result = false;
//      if (v.getPropertyCount() > 0) {
//        Object o = v.getProperty(PREDICATE_PROPERTY_KEY);
//        result = (o != null && o.equals(PREDICATE_PROPERTY_VALUE));
//      }
//      return result;
//    }
//  };

  public class SelectMapper extends TableMapper<LongWritable, PairWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context)
      throws IOException, InterruptedException {
      Map<String, Object> vertexProps = VERTEX_HANDLER.readProperties(value);

      int calcValue = 0;
      if (vertexProps.containsKey(PROJECTION_PROPERTY_KEY1)) {
        calcValue += (int) vertexProps.get(PROJECTION_PROPERTY_KEY1);
      }
      if (vertexProps.containsKey(PROJECTION_PROPERTY_KEY2)) {
        calcValue -= (int) vertexProps.get(PROJECTION_PROPERTY_KEY2);
      }
      if (calcValue != 0) {
        IntWritable calcValueWritable = new IntWritable(calcValue);

        boolean predicate = vertexProps.containsKey(PREDICATE_PROPERTY_KEY)
          && PREDICATE_PROPERTY_VALUE
          .equals(vertexProps.get(PREDICATE_PROPERTY_KEY));
        for (Long graph : VERTEX_HANDLER.readGraphs(value)) {
          if (predicate) {
            context.write(new LongWritable(graph), new PairWritable(TRUE,
              calcValueWritable));
          } else {
            context.write(new LongWritable(graph), new PairWritable(FALSE,
              calcValueWritable));
          }
        }
      }
    }
  }

  public class AggregateReducer extends TableReducer<LongWritable,
    PairWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<PairWritable> values,
                          Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      boolean predicate = false;
      // calculate aggregate and check if the graph matches the predicate
      for (PairWritable pair : values) {
        sum = sum + pair.getValue().get();
        if (pair.getPredicateResult().get() == true) {
          predicate = true;
        }
      }

      if (predicate) {
        Graph g = new MemoryGraph(key.get());
        g.addProperty(AGGREGATION_RESULT_PROPERTY_KEY, sum);
        Put put = new Put(Bytes.toBytes(key.get()));
        put = GRAPH_HANDLER.writeVertices(put, g);
        context.write(null, put);
      }
    }
  }

}
