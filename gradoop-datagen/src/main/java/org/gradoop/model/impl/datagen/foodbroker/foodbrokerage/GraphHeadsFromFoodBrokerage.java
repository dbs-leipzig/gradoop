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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

/**
 * Map function which creates graph heads from all graph ids created by the
 * foodbrokerage process.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphHeadsFromFoodBrokerage<G extends EPGMGraphHead, V extends
  EPGMVertex, E extends EPGMEdge>
  implements MapFunction<Tuple2<Set<V>, Set<E>>, G> {

  /**
   * EPGM graph head factory
   */
  private EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Valued constructor.
   *
   * @param graphHeadFactory EPGM graph head factory
   */
  public GraphHeadsFromFoodBrokerage(EPGMGraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public G map(Tuple2<Set<V>, Set<E>> tuple) throws Exception {
    return graphHeadFactory.initGraphHead(tuple.f0.iterator().next()
      .getGraphIds().iterator().next());
  }
}
