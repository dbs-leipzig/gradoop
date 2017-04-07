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
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Adds a GraphElement to a graph
 * @param <E> GraphElement
 */
@FunctionAnnotation.ForwardedFields("f1.id -> id; f1.properties -> properties; f1.label -> label")
public class AddElementToGraph2
  implements CoGroupFunction<Tuple2<String,Edge>, Tuple2<String,GradoopId>, Edge> {

  @Override
  public void coGroup(Iterable<Tuple2<String, Edge>> left,
    Iterable<Tuple2<String, GradoopId>> right, Collector<Edge> collector) throws Exception {
    for (Tuple2<String, Edge> x : left) {
      Edge e = x.f1;
      for (Tuple2<String, GradoopId> y : right) {
        if (!e.getGraphIds().contains(y.f1)) {
          e.addGraphId(y.f1);
        }
      }
      collector.collect(e);
    }
  }
}
