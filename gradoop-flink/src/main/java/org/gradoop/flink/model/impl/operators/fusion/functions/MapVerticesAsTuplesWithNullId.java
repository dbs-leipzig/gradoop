
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Maps vertices that are not associated to a graph id
 * to a null id.
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class MapVerticesAsTuplesWithNullId
  implements MapFunction<Vertex, Tuple2<Vertex, GradoopId>> {

  /**
   * Reusable returned element
   */
  private final Tuple2<Vertex, GradoopId> reusable;

  /**
   * Default constructor
   */
  public MapVerticesAsTuplesWithNullId() {
    reusable = new Tuple2<>();
    reusable.f1 = GradoopId.NULL_VALUE;
  }

  @Override
  public Tuple2<Vertex, GradoopId> map(Vertex value) throws Exception {
    reusable.f0 = value;
    return reusable;
  }
}
