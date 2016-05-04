package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Initializes an {@link EPGMVertex} from a given {@link GradoopId} triple.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("f0->id;f1->sourceId;f2->targetId")
public class EdgeFromId<E extends EPGMEdge>
  implements MapFunction<Tuple3<GradoopId, GradoopId, GradoopId>, E>,
  ResultTypeQueryable<E> {

  /**
   * EPGM edge factory
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  public EdgeFromId(EPGMEdgeFactory<E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * Initializes an {@link EPGMEdge} from a given {@link GradoopId} triple. The
   * triple consists of edge id, source vertex id and target vertex id.
   *
   * @param idTriple triple containing (in that order) edge id, source vertex
   *                 id, target vertex id
   * @return EPGM edge
   * @throws Exception
   */
  @Override
  public E map(Tuple3<GradoopId, GradoopId, GradoopId> idTriple)
    throws Exception {
    return edgeFactory.initEdge(idTriple.f0, idTriple.f1, idTriple.f2);
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
