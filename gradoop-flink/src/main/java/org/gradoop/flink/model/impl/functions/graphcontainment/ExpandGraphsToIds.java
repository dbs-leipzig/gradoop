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

package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Takes a graph element as input and collects all graph ids the element is
 * contained in.
 *
 * graph-element -> {graph id 1, graph id 2, ..., graph id n}
 *
 * @param <GE> EPGM graph element (i.e. vertex / edge)
 */
@FunctionAnnotation.ReadFields("graphIds")
public class ExpandGraphsToIds<GE extends GraphElement>
  implements FlatMapFunction<GE, GradoopId> {

  @Override
  public void flatMap(GE ge, Collector<GradoopId> collector) {
    for (GradoopId gradoopId : ge.getGraphIds()) {
      collector.collect(gradoopId);
    }
  }
}
