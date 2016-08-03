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
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * (graphId, sourceValue, targetValue, edgeLabel) => edge
 *
 * @param <E> edge type
 */
public class FullEdge<E extends EPGMEdge> implements
  MapFunction<Tuple4<GradoopId, GradoopId, GradoopId, String>, E> {

  /**
   * edge factory
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * constructor
   *
   * @param edgeFactory edge factory
   */
  public FullEdge(EPGMEdgeFactory<E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public E map(
    Tuple4<GradoopId, GradoopId, GradoopId, String> edge) throws Exception {

    return edgeFactory.createEdge(
      edge.f3, edge.f1, edge.f2, GradoopIdSet.fromExisting(edge.f0));
  }
}
