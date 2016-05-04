package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Initializes an {@link EPGMVertex} from a given {@link GradoopId}.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("f0->id")
public class VertexFromId<V extends EPGMVertex>
  implements MapFunction<Tuple1<GradoopId>, V>, ResultTypeQueryable<V> {

  /**
   * EPGM vertex factory
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Create new function.
   *
   * @param vertexFactory EPGM vertex factory
   */
  public VertexFromId(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * Initializes an {@link EPGMVertex} from a given {@link GradoopId}.
   *
   * @param gradoopId Gradoop identifier
   * @return EPGM vertex
   * @throws Exception
   */
  @Override
  public V map(Tuple1<GradoopId> gradoopId) throws Exception {
    return vertexFactory.initVertex(gradoopId.f0);
  }

  @Override
  public TypeInformation<V> getProducedType() {
    return TypeExtractor.createTypeInfo(vertexFactory.getType());
  }
}
