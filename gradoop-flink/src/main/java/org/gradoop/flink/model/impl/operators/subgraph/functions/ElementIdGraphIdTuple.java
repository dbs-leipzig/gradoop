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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a tuple containing the id of the element and the id of one graph for
 * each graph the element is contained in.
 * (id:el{id1, id2}) => (id, id1),(id, id2)
 * @param <EL> epgm graph element type
 */

@FunctionAnnotation.ReadFields("graphIds")
@FunctionAnnotation.ForwardedFields("id->f0")
public class ElementIdGraphIdTuple<EL extends GraphElement>
  implements FlatMapFunction<EL, Tuple2<GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    EL element,
    Collector<Tuple2<GradoopId, GradoopId>> collector) throws Exception {

    for (GradoopId graph : element.getGraphIds()) {
      collector.collect(new Tuple2<>(element.getId(), graph));
    }
  }
}
