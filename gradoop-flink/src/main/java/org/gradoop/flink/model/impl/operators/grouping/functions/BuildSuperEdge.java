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

import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import org.gradoop.common.util.GConstants;

import java.io.IOException;
import java.util.List;

/**
 * Creates a single {@link EdgeGroupItem} from a set of group items.
 *
 * @see ReduceEdgeGroupItems
 * @see CombineEdgeGroupItems
 */
abstract class BuildSuperEdge extends BuildBase {

  /**
   * Creates group reducer / combiner
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param valueAggregators  aggregate functions for edge values
   */
  public BuildSuperEdge(List<String> groupPropertyKeys,
    boolean useLabel,
    List<PropertyValueAggregator> valueAggregators) {
    super(groupPropertyKeys, useLabel, valueAggregators);
  }

  /**
   * Iterators the given edge group items and build a group representative item.
   *
   * @param edgeGroupItems edge group items
   * @return group representative item
   */
  protected EdgeGroupItem reduceInternal(
    Iterable<EdgeGroupItem> edgeGroupItems) throws IOException {

    EdgeGroupItem edgeGroupItem = new EdgeGroupItem();

    boolean firstElement = true;

    for (EdgeGroupItem edge : edgeGroupItems) {
      if (firstElement) {
        edgeGroupItem.setSourceId(edge.getSourceId());
        edgeGroupItem.setTargetId(edge.getTargetId());
        if (useLabel()) {
          edgeGroupItem.setGroupLabel(edge.getGroupLabel());
        } else {
          edgeGroupItem.setGroupLabel(GConstants.DEFAULT_EDGE_LABEL);
        }
        edgeGroupItem.setGroupingValues(edge.getGroupingValues());
        firstElement = false;
      }

      if (doAggregate()) {
        aggregate(edge.getAggregateValues());
      } else {
        // no need to iterate further
        break;
      }
    }

    edgeGroupItem.setAggregateValues(getAggregateValues());

    return edgeGroupItem;
  }
}
