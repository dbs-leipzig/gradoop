/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an {@link Vertex} from a given {@link GradoopId} triple.
 *
 * (edgeId, sourceId, targetId) -> edge
 *
 * Forwarded fields:
 *
 * f0->id:        edge id
 * f1->sourceId:  source vertex id
 * f2->targetId:  target vertex id
 */
@FunctionAnnotation.ForwardedFields("f0->id;f1->sourceId;f2->targetId")
public class EdgeFromIds implements
  MapFunction<Tuple3<GradoopId, GradoopId, GradoopId>, Edge>,
  ResultTypeQueryable<Edge> {

  /**
   * EPGM edge factory
   */
  private final EdgeFactory edgeFactory;

  /**
   * Constructor
   *
   * @param edgeFactory EPGM edge factory
   */
  public EdgeFromIds(EdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * Initializes an {@link Edge} from a given {@link GradoopId} triple. The
   * triple consists of edge id, source vertex id and target vertex id.
   *
   * @param idTriple triple containing (in that order) edge id, source vertex
   *                 id, target vertex id
   * @return EPGM edge
   * @throws Exception
   */
  @Override
  public Edge map(Tuple3<GradoopId, GradoopId, GradoopId> idTriple) throws
    Exception {
    return edgeFactory.initEdge(idTriple.f0, idTriple.f1, idTriple.f2);
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<Edge> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
