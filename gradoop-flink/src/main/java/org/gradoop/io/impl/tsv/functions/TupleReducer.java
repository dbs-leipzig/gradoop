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
      collector) throws
    Exception {

    Map<String, GradoopId> sourceDuplicates = new HashMap<>();
    Map<String, GradoopId> targetDuplicates = new HashMap<>();

    for (Tuple6<String, GradoopId, String, String, GradoopId, String> tup: iterable) {

      boolean s = sourceDuplicates.containsKey(tup.f0);
      boolean t = targetDuplicates.containsKey(tup.f3);

      if (!(s || t)){
        collector.collect(tup);
      }else{
        if(s){
          collector
            .collect(
              new Tuple6<>(tup.f0, sourceDuplicates.get(tup.f0),
                tup.f2, tup.f3, tup.f4, tup.f5));
        }
        if(t){
          collector
            .collect(
              new Tuple6<>(tup.f0, tup.f1, tup.f2, tup.f3,
                targetDuplicates.get(tup.f3), tup.f5));
        }
      }
      sourceDuplicates.put(tup.f0, tup.f1);
      targetDuplicates.put(tup.f3, tup.f4);
    }
  }
}
