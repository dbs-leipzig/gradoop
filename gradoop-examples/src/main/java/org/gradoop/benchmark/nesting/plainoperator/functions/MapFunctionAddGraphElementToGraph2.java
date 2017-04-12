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

package org.gradoop.benchmark.nesting.plainoperator.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * A variant of the AddToGraph, where we have no graph head
 * but a graph id
 * @param <K> element receiving the new graph id
 *
 */
public class MapFunctionAddGraphElementToGraph2<K extends GraphElement> implements MapFunction<K, K> {

  /**
   * Graph Id that has to be added
   */
  private final GradoopId newGraphId;

  /**
   * Default constructor
   * @param newGraphId  Graph Id that has to be added
   */
  public MapFunctionAddGraphElementToGraph2(GradoopId newGraphId) {
    this.newGraphId = newGraphId;
  }

  @Override
  public K map(K value) throws Exception {
    value.addGraphId(newGraphId);
    return value;
  }
}
