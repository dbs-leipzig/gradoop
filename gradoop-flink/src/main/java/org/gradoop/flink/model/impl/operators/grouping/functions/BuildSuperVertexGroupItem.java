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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperVertexGroupItem;

import java.util.Iterator;
import java.util.Set;

public class BuildSuperVertexGroupItem
  implements GroupReduceFunction<Tuple2<Set<GradoopId>,GradoopId>, SuperVertexGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private final SuperVertexGroupItem reuseSuperVertexGroupItem;


  public BuildSuperVertexGroupItem() {
    reuseSuperVertexGroupItem = new SuperVertexGroupItem();
    reuseSuperVertexGroupItem.setAggregateValues(PropertyValueList.createEmptyList());
    reuseSuperVertexGroupItem.setGroupingValues(PropertyValueList.createEmptyList());
  }

  @Override
  public void reduce(Iterable<Tuple2<Set<GradoopId>, GradoopId>> iterable,
    Collector<SuperVertexGroupItem> collector) throws Exception {

    Iterator<Tuple2<Set<GradoopId>, GradoopId>> iterator = iterable.iterator();
    Tuple2<Set<GradoopId>, GradoopId> tuple = iterator.next();
    GradoopId superVertexId;
    Set<GradoopId> vertices;

    vertices = tuple.f0;
    if (vertices.size() != 1) {
      superVertexId = GradoopId.get();
    } else {
      superVertexId = vertices.iterator().next();
    }
    reuseSuperVertexGroupItem.setVertexIds(vertices);
    reuseSuperVertexGroupItem.setSuperVertexId(superVertexId);
    reuseSuperVertexGroupItem.setSuperEdgeId(tuple.f1);

    collector.collect(reuseSuperVertexGroupItem);

    while (iterator.hasNext()) {
      reuseSuperVertexGroupItem.setSuperEdgeId(iterator.next().f1);
      collector.collect(reuseSuperVertexGroupItem);
    }
  }
}



