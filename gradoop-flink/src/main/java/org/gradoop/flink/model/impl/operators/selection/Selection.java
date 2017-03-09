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

package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.GraphCollection;

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
