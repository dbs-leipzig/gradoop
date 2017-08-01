/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.GraphCollection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Filter logical graphs from a graph collection based on their associated graph
 * head.
 */
public class Selection extends SelectionBase {

  /**
   * User-defined predicate function
   */
  private final FilterFunction<GraphHead> predicate;

  /**
   * Creates a new Selection operator.
   *
   * @param predicate user-defined predicate function
   */
  public Selection(FilterFunction<GraphHead> predicate) {
    this.predicate = checkNotNull(predicate, "Predicate function was null");
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // find graph heads matching the predicate
    DataSet<GraphHead> graphHeads = collection
      .getGraphHeads()
      .filter(predicate);

    return selectVerticesAndEdges(collection, graphHeads);
  }

  @Override
  public String getName() {
    return Selection.class.getName();
  }
}
