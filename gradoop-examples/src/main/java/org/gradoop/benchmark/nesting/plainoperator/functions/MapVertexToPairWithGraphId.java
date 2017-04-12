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

package org.gradoop.benchmark.nesting.plainoperator.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Demultiplexes a vertex by associating its graphId
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class MapVertexToPairWithGraphId implements FlatMapFunction<Vertex, Tuple2<Vertex,   GradoopId>> {

  /**
   * Reusable element ot be returned
   */
  private final Tuple2<Vertex,   GradoopId> reusableTuple;

  /**
   * Default constructor
   */
  public MapVertexToPairWithGraphId() {
    reusableTuple = new Tuple2<>();
  }

  @Override
  public void flatMap(Vertex value, Collector<Tuple2<Vertex, GradoopId>> out) throws Exception {
    if (value != null) {
      for (GradoopId id : value.getGraphIds()) {
        reusableTuple.f0 = value;
        reusableTuple.f1 = id;
        out.collect(reusableTuple);
      }
    }
  }
}
