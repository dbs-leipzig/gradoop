package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.HashMap;
import java.util.Map;

/**
 * GroupReduceFunction to reduce duplicated (original) source and target ids
 * in the tuples (edges).
 */
public class TupleReducer implements GroupReduceFunction<Tuple6<String,
  GradoopId, String, String, GradoopId, String>, Tuple6<String,
    GradoopId, String, String, GradoopId, String>> {

  /**
   * For every duplicated (original) id one gradoopID will be set.
   *
   * @param iterable    tuple set
   * @param collector   reduce collector
   * @throws Exception
   */
  @Override
  public void reduce(
    Iterable<Tuple6<String, GradoopId, String, String, GradoopId, String>>
      iterable,
    Collector<Tuple6<String, GradoopId, String, String, GradoopId, String>>
      collector)
    throws Exception {

    Map<String, GradoopId> sourceDuplicates = new HashMap<>();
    Map<String, GradoopId> targetDuplicates = new HashMap<>();

    for (Tuple6<String, GradoopId, String, String, GradoopId, String>
      tuple: iterable) {

      boolean s = sourceDuplicates.containsKey(tuple.f0);
      boolean t = targetDuplicates.containsKey(tuple.f3);

      if (!(s || t)){
        collector.collect(tuple);
      }else{
        if(s){
          tuple.f1 = sourceDuplicates.get(tuple.f0);
          collector.collect(tuple);
        }
        if(t){
          tuple.f4 = targetDuplicates.get(tuple.f3);
          collector.collect(tuple);

        }
      }
      sourceDuplicates.put(tuple.f0, tuple.f1);
      targetDuplicates.put(tuple.f3, tuple.f4);
    }
  }
}
