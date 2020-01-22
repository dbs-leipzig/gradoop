/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an {@link Edge} from a given {@link GradoopId} triple.
 * The triple contains (in that order) {@code edge id}, {@code source vertex id} and
 * {@code target vertex id}
 * <p>
 * Forwarded fields:
 * <p>
 * {@code f0->id:        edge id}
 * <p>
 * {@code f1->sourceId:  source vertex id}
 * <p>
 * {@code f2->targetId:  target vertex id}
 *
 * @param <E> The produced edge type.
 */
@FunctionAnnotation.ForwardedFields("f0->id;f1->sourceId;f2->targetId")
public class EdgeFromIds<E extends Edge> implements MapFunction<Tuple3<GradoopId, GradoopId, GradoopId>, E>,
  ResultTypeQueryable<E> {

  /**
   * The type of the produced edge.
   */
  private final TypeInformation<E> edgeType;

  /**
   * Reduce object instantiations.
   */
  private final E reuseEdge;

  /**
   * Constructor.
   *
   * @param edgeFactory The edge factory.
   */
  public EdgeFromIds(EdgeFactory<E> edgeFactory) {
    edgeType = TypeInformation.of(edgeFactory.getType());
    reuseEdge = edgeFactory.createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
  }

  @Override
  public E map(Tuple3<GradoopId, GradoopId, GradoopId> idTriple) {
    reuseEdge.setId(idTriple.f0);
    reuseEdge.setSourceId(idTriple.f1);
    reuseEdge.setTargetId(idTriple.f2);
    return reuseEdge;
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return edgeType;
  }
}
