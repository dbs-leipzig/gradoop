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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

import java.util.HashSet;

/**
 * Created by vasistas on 09/03/17.
 */
public class CoGroupIdsWithActualElements<X extends GraphElement> implements
  CoGroupFunction<Tuple2<GradoopId, GradoopId>, X, X> {

  HashSet<GradoopId> appearing;

  public CoGroupIdsWithActualElements() {
    appearing = new HashSet<>();
  }

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, GradoopId>> first, Iterable<X> second,
    Collector<X> out) throws Exception {
      appearing.clear();
      first.forEach(x -> appearing.add(x.f0));
      for (X y : second) {
        /*
        XXX: ???
        for (GradoopId x : appearing) {
          y.addGraphId(x);
        }
        */
        out.collect(y);
      }
  }
}
