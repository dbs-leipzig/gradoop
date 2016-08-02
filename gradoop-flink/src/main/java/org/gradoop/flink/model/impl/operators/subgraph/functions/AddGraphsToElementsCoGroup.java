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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * CoGroups tuples containing gradoop ids and gradoop id sets with graph
 * elements with the same gradoop ids and adds the gradoop id sets to each
 * element.
 * @param <EL> epgm graph element type
 */

@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;properties")
public class AddGraphsToElementsCoGroup<EL extends GraphElement>
  implements CoGroupFunction<Tuple2<GradoopId, GradoopIdSet>, EL, EL> {

  @Override
  public void coGroup(
    Iterable<Tuple2<GradoopId, GradoopIdSet>> graphs,
    Iterable<EL> elements,
    Collector<EL> collector) throws Exception {
    for (EL element : elements) {
      for (Tuple2<GradoopId, GradoopIdSet> graphSet : graphs) {
        element.getGraphIds().addAll(graphSet.f1);
      }
      collector.collect(element);
    }
  }
}
