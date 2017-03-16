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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

/**
 * Returns a tuple which assigns each source/target set of gradoop ids the edge id.
 */
public class PrepareSuperVertexGroupItem
  implements FlatMapFunction<SuperEdgeGroupItem, SuperVertexGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private SuperVertexGroupItem reuseSuperVertexGroupItem;

  /**
   * Constructor to initialize object.
   */
  public PrepareSuperVertexGroupItem() {
    reuseSuperVertexGroupItem = new SuperVertexGroupItem();
    reuseSuperVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
//    reuseSuperVertexGroupItem.setGroupLabel("");
    reuseSuperVertexGroupItem.setGroupingValues(PropertyValueList.createEmptyList());
    reuseSuperVertexGroupItem.setAggregateValues(PropertyValueList.createEmptyList());

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(SuperEdgeGroupItem superEdgeGroupItem,
    Collector<SuperVertexGroupItem> collector) throws Exception {

    reuseSuperVertexGroupItem.setVertexIds(superEdgeGroupItem.getSourceIds());
    reuseSuperVertexGroupItem.setSuperEdgeId(superEdgeGroupItem.getEdgeId());
    collector.collect(reuseSuperVertexGroupItem);

    reuseSuperVertexGroupItem.setVertexIds(superEdgeGroupItem.getTargetIds());
    reuseSuperVertexGroupItem.setSuperEdgeId(superEdgeGroupItem.getEdgeId());
    collector.collect(reuseSuperVertexGroupItem);
  }
}
