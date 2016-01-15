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

package org.gradoop.model.impl.operators.projection.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.api.functions.ProjectionFunction;
import org.gradoop.model.impl.id.GradoopId;


/**
 * Applies a given projection function on the input EPGM vertex. Returns
 * a Tuple2 of the input and the output vertex.
 *
 * @param <V> EPGM vertex type
 */
public class ProjectionVertexMapper<V extends EPGMVertex>
  implements MapFunction<V, Tuple2<GradoopId, V>> {
  /**
   * Vertex projection function
   */
  private ProjectionFunction<V> func;

  /**
   * Factory for creation of new vertices
   */
  private EPGMVertexFactory<V> factory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<GradoopId, V> reuseTuple = new Tuple2<>();

  /**
   * Create a new ProjectionVertexMapper
   *
   * @param factory factory for creation of new vertices
   * @param func the projection function
   */
  public ProjectionVertexMapper(
    EPGMVertexFactory<V> factory,
    ProjectionFunction<V> func) {
    this.factory = factory;
    this.func = func;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, V> map(V vertex) throws Exception {
    this.reuseTuple.setFields(
      vertex.getId(),
      func.execute(vertex, factory.createVertex()));

    return reuseTuple;
  }
}
