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

package org.gradoop.model.impl.operators.grouping.functions;

import org.gradoop.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.util.GConstants;

import java.util.List;

/**
 * Creates a single {@link EdgeGroupItem} from a set of group items.
 *
 * @see ReduceEdgeGroupItems
 * @see CombineEdgeGroupItems
 */
public abstract class BuildSuperEdge extends BuildBase {

  /**
   * Creates group reducer / combiner
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param valueAggregator   aggregate function for edge values
   */
  public BuildSuperEdge(List<String> groupPropertyKeys,
    boolean useLabel,
    PropertyValueAggregator valueAggregator) {
    super(groupPropertyKeys, useLabel, valueAggregator);
  }

  /**
   * Iterators the given edge group items and build a group representative item.
   *
   * @param edgeGroupItems edge group items
   * @return group representative item
   */
  protected EdgeGroupItem reduceInternal(
    Iterable<EdgeGroupItem> edgeGroupItems) {

    EdgeGroupItem edgeGroupItem = new EdgeGroupItem();

    boolean firstElement = true;

    for (EdgeGroupItem e : edgeGroupItems) {
      if (firstElement) {
        edgeGroupItem.setSourceId(e.getSourceId());
        edgeGroupItem.setTargetId(e.getTargetId());
        if (useLabel()) {
          edgeGroupItem.setGroupLabel(e.getGroupLabel());
        } else {
          edgeGroupItem.setGroupLabel(GConstants.DEFAULT_EDGE_LABEL);
        }
        edgeGroupItem.setGroupPropertyValues(e.getGroupPropertyValues());
        firstElement = false;
      }

      if (doAggregate()) {
        aggregate(e.getGroupAggregate());
      } else {
        // no need to iterate further
        break;
      }
    }

    edgeGroupItem.setGroupAggregate(getAggregate());

    return edgeGroupItem;
  }
}
