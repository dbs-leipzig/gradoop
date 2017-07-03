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

import java.io.IOException;

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
   * @param useLabel    use edge label
   */
  public BuildSuperEdge(boolean useLabel) {
    super(useLabel);
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
    boolean firstElement        = true;

    for (EdgeGroupItem edge : edgeGroupItems) {
      if (firstElement) {
        edgeGroupItem.setSourceId(edge.getSourceId());
        edgeGroupItem.setTargetId(edge.getTargetId());
        edgeGroupItem.setGroupLabel(edge.getGroupLabel());
        edgeGroupItem.setGroupingValues(edge.getGroupingValues());
        edgeGroupItem.setLabelGroup(edge.getLabelGroup());
        firstElement = false;
      }

      if (doAggregate(edgeGroupItem.getLabelGroup().getAggregators())) {
        aggregate(edge.getAggregateValues(), edgeGroupItem.getLabelGroup().getAggregators());
      } else {
        // no need to iterate further
        break;
      }
    }

    edgeGroupItem.setAggregateValues(
      getAggregateValues(edgeGroupItem.getLabelGroup().getAggregators()));
    return edgeGroupItem;
  }
}
