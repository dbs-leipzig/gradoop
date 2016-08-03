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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an {@link Vertex} from a given {@link GradoopId}.
 */
@FunctionAnnotation.ForwardedFields("f0->id")
public class VertexFromId implements
  MapFunction<Tuple1<GradoopId>, Vertex>,
  ResultTypeQueryable<Vertex> {

  /**
   * EPGM vertex factory
   */
  private final VertexFactory vertexFactory;

  /**
   * Create new function.
   *
   * @param vertexFactory EPGM vertex factory
   */
  public VertexFromId(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * Initializes an {@link Vertex} from a given {@link GradoopId}.
   *
   * @param gradoopId Gradoop identifier
   * @return EPGM vertex
   * @throws Exception
   */
  @Override
  public Vertex map(Tuple1<GradoopId> gradoopId) throws Exception {
    return vertexFactory.initVertex(gradoopId.f0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<Vertex> getProducedType() {
    return TypeExtractor.createTypeInfo(vertexFactory.getType());
  }
}
