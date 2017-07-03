package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Takes a tuple 2, containing an object and a gradoop id set, and creates one
 * new tuple 2 of the object and a gradoop id for each gradoop id in the set.
 *
 * @param <T> f0 type
 */
@FunctionAnnotation.ReadFields("f1")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class ExpandGradoopIds<T> implements FlatMapFunction
  <Tuple2<T, GradoopIdList>, Tuple2<T, GradoopId>> {

  @Override
  public void flatMap(
    Tuple2<T, GradoopIdList> pair,
    Collector<Tuple2<T, GradoopId>> collector) throws Exception {

    T firstField = pair.f0;

    for (GradoopId toId : pair.f1) {
      collector.collect(new Tuple2<>(firstField, toId));
    }

  }
}
