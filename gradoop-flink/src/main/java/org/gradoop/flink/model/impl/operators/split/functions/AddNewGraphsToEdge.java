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

package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Adds new graph id's to the edge if source and target vertex are part of
 * the same graph. Filters all edges between graphs.
 *
 * @param <E> EPGM edge Type
 */
@FunctionAnnotation.ForwardedFields("f0.id->id;f0.sourceId->sourceId;" +
  "f0.targetId->targetId;f0.label->label;f0.properties->properties")
public class AddNewGraphsToEdge<E extends Edge>
  implements FlatMapFunction<Tuple3<E, GradoopIdList, GradoopIdList>, E> {

  @Override
  public void flatMap(
    Tuple3<E, GradoopIdList, GradoopIdList> triple,
    Collector<E> collector) {
    GradoopIdList sourceGraphs = triple.f1;
    GradoopIdList targetGraphs = triple.f2;
    GradoopIdList graphsToBeAdded = new GradoopIdList();

    boolean filter = false;
    for (GradoopId id : sourceGraphs) {
      if (targetGraphs.contains(id)) {
        graphsToBeAdded.add(id);
        filter = true;
      }
    }

    if (filter) {
      E edge = triple.f0;
      edge.getGraphIds().addAll(graphsToBeAdded);
      collector.collect(edge);
    }
  }
}
