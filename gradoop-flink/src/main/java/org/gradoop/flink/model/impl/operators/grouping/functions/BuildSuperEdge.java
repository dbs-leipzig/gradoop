/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
