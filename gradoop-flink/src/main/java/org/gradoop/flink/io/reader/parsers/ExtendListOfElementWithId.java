package org.gradoop.flink.io.reader.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;

import java.util.List;

/**
 * Associates to each group a different id
 */
@FunctionAnnotation.ForwardedFieldsSecond("* -> f1")
public class ExtendListOfElementWithId<Element> implements
  FlatMapFunction<List<Element>, Tuple2<GradoopId, Element>> {

  /**
   * Reusable element
   */
  private final Tuple2<GradoopId,Element> reusable;

  /**
   * Defines the graph head factory for defining new graphs
   */
  private final GraphHeadFactory fact;

  /**
   * Default constructor
   */
  public ExtendListOfElementWithId(GraphHeadFactory fact) {
    reusable = new Tuple2<>();
    this.fact = fact;
  }

  @Override
  public void flatMap(List<Element> value, Collector<Tuple2<GradoopId, Element>> out) throws
    Exception {
    GradoopId id = fact.createGraphHead().getId();
    reusable.f0 = id;
    for (Element x : value) {
      reusable.f1 = x;
      out.collect(reusable);
    }
  }
}
