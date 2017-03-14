package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Associate to each vertex id its graph id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1 -> f1")
public class CreateIdGraphDatabaseVertices<Element> implements
  JoinFunction<Tuple2<GradoopId, Element>,
               Tuple3<Element,GradoopId,Vertex>,
               Tuple2<GradoopId,GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<GradoopId,GradoopId> reusable;

  /**
   * Default constructor
   */
  public CreateIdGraphDatabaseVertices() {
    reusable = new Tuple2<>();
  }

  @Override
  public Tuple2<GradoopId, GradoopId> join(Tuple2<GradoopId, Element> first,
    Tuple3<Element, GradoopId, Vertex> second) throws Exception {
    reusable.f0 = first.f0;
    reusable.f1 = second.f1;
    return reusable;
  }

}
