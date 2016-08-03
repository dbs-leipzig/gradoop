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

package org.gradoop.flink.algorithms.fsm.gspan.decoders.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * (graphId, vertexId, vertexLabel) => vertex
 *
 * @param <V> vertex type
 */
public class FullVertex<V extends EPGMVertex> implements
  MapFunction<Tuple3<GradoopId, GradoopId, String>, V> {

  /**
   * vertex factory
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * constructor
   *
   * @param vertexFactory vertex factory
   */
  public FullVertex(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public V map(
    Tuple3<GradoopId, GradoopId, String> gidVidLabel) throws Exception {
    return vertexFactory.initVertex(
      gidVidLabel.f1, gidVidLabel.f2,
      GradoopIdSet.fromExisting(gidVidLabel.f0));
  }
}
