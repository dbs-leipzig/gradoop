package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst
  ("f1 -> sourceId; f3 -> label; f4 -> properties; f5 -> graphIds")
@FunctionAnnotation.ForwardedFieldsSecond("f1.id -> targetId")
public class CreateEdge<K extends Comparable<K>> implements
  JoinFunction<Tuple6<K, GradoopId, K, String, Properties, GradoopIdList>, Tuple2<K, Vertex>, Edge> {

  private final EdgeFactory factory;

  public CreateEdge(EdgeFactory factory) {
    this.factory = factory;
  }

  @Override
  public Edge join(Tuple6<K, GradoopId, K, String, Properties, GradoopIdList> first,
    Tuple2<K, Vertex> second) throws Exception {
    return factory.createEdge(first.f3, first.f1, second.f1.getId(), first.f4, first.f5);
  }
}
