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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Merging graph heads
 *
 * Created by vasistas on 01/02/17.
 */
public class OplusHeads extends OplusSemiConcrete<GraphHead> implements Serializable {

  /**
   * Reusable field
   */
  private final GraphHead gh;

  /**
   * Default constructor
   * @param transformation  Graph Heads labels' concatenation function
   */
  public OplusHeads(Function<Tuple2<String, String>, String> transformation) {
    super(transformation);
    gh = new GraphHead();
  }

  @Override
  public GraphHead supplyEmpty() {
    return gh;
  }

}
