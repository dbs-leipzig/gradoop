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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Used for both updating vertices and edges. Sets the vertices to be
 * returned as appearing in the right GraphId elements
 *
 * @param <X> the GraphElement
 */
public class CoGroupIdsWithActualElements<X extends GraphElement> implements
  CoGroupFunction<Tuple2<GradoopId, GradoopId>, X, X> {

  /**
   * reusable element
   */
  private GradoopIdList appearing;

  /**
   * Default constructor
   */
  public CoGroupIdsWithActualElements() {
    appearing = new GradoopIdList();
  }

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, GradoopId>> first, Iterable<X> second,
    Collector<X> out) throws Exception {
    appearing.clear();
    first.forEach(x -> appearing.add(x.f0));
    if (appearing.isEmpty()) {
      return;
    }
    for (X y : second) {
      for (GradoopId z : appearing) {
        if (!y.getGraphIds().contains(z)) {
          y.addGraphId(z);
        }
      }
      out.collect(y);
    }
  }
}
