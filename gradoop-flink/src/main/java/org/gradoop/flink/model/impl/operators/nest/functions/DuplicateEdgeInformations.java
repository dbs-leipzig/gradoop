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
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

import java.util.Iterator;

/**
 * Created by vasistas on 09/03/17.
 */
public class DuplicateEdgeInformations implements CoGroupFunction<Edge, Hexaplet, Edge> {

  @Override
  public void coGroup(Iterable<Edge> first, Iterable<Hexaplet> second, Collector<Edge> out) throws
    Exception {
    // There should be just one edge with the same id
    Iterator<Edge> it = first.iterator();
    if (it.hasNext()) {
      Edge e = it.next();
      for (Hexaplet y : second) {
        if (!y.f4.equals(GradoopId.NULL_VALUE)) {
          e.setId(y.f4);
          e.setSourceId(y.getSource());
          e.setTargetId(y.getTarget());
          out.collect(e);
        }
      }
    }
  }

}
