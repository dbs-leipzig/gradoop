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

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Iterator;

/**
 * Maps a quad into a <newgraph,vertex> paid to be used within the IdGraphDatabase
 */
public class CollectVertices extends
  RichGroupReduceFunction<Tuple3<GradoopId, GradoopId, GradoopId>, GradoopId> {

  @Override
  public void reduce(Iterable<Tuple3<GradoopId, GradoopId, GradoopId>> values,
                     Collector<GradoopId> out) throws Exception {
    Iterator<Tuple3<GradoopId, GradoopId, GradoopId>> it = values.iterator();
    if (it.hasNext()) {
      Tuple3<GradoopId, GradoopId, GradoopId> x;
      GradoopId y = null;
      do {
        x = it.next();
        // First iteration
        if (y == null) {
          y = x.f1;
        }
        if (!y.equals(GradoopId.NULL_VALUE)) {
          // If the vertices are summarized into a graph collection id, return it just once
          out.collect(y);
          break;
        } else {
          out.collect(x.f2);
        }
      } while (it.hasNext() && (x.f1.equals(GradoopId.NULL_VALUE)));
    }
  }
}

