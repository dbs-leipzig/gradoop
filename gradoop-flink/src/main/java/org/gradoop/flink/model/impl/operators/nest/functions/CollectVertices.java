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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

import java.util.Iterator;

/**
 * Maps a quad into a <newgraph,vertex> paid to be used within the IdGraphDatabase
 */
public class CollectVertices extends RichGroupReduceFunction<Hexaplet, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<GradoopId, GradoopId> reusable;


  /**
   * Default constructor
   * @param newGraphId  New Graph Id to be associated to the new graph
   */
  public CollectVertices(GradoopId newGraphId) {
    reusable = new Tuple2<>();
    reusable.f0 = newGraphId;
  }

  @Override
  public void reduce(Iterable<Hexaplet> values,
    Collector<Tuple2<GradoopId, GradoopId>> out) throws Exception {
    Iterator<Hexaplet> it = values.iterator();
    if (it.hasNext()) {
      Hexaplet x;
      GradoopId y = null;
      do {
        x = it.next();
        // First iteration
        if (y == null) {
          y = x.f4;
        }
        if (!y.equals(GradoopId.NULL_VALUE)) {
          // If the vertices are summarized into a graph collection id, return it just once
          reusable.f1 = y;
          out.collect(reusable);
          break;
        } else {
          reusable.f1 = x.f1;
          out.collect(reusable);
        }
      } while (it.hasNext() && (x.f4.equals(GradoopId.NULL_VALUE)));
    }
  }
}

