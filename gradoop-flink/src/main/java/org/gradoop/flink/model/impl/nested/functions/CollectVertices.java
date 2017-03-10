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

package org.gradoop.flink.model.impl.nested.functions;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.tuples.Quad;

import java.util.Iterator;

/**
 * Created by vasistas on 09/03/17.
 */
public class CollectVertices extends RichGroupReduceFunction<Quad, Tuple2<GradoopId, GradoopId>> {

  private final Tuple2<GradoopId,GradoopId> reusable;
  private final GradoopId newGraphId;

  public CollectVertices(GradoopId newGraphId) {
    reusable = new Tuple2<>();
    this.newGraphId = newGraphId;
  }

  @Override
  public void reduce(Iterable<Quad> values,
    Collector<Tuple2<GradoopId, GradoopId>> out) throws Exception {
    Iterator<Quad> it = values.iterator();
    if (it.hasNext()) {
      Quad x;
      do {
        x = it.next();
        reusable.f1 = x.getVertexId();
                                                          /////
        reusable.f0 = x.f4.equals(GradoopId.NULL_VALUE) ? x.f0 : x.f4;
        out.collect(reusable);
      } while (it.hasNext() && (x.f4.equals(GradoopId.NULL_VALUE)));
    }
  }
}

