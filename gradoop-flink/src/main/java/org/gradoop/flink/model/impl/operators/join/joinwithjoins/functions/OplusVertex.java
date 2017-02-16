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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Merging vertices together
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class OplusVertex extends OplusSemiConcrete<Vertex> implements Serializable {

  /**
   * reusable vertex while generating vertices
   */
  private final Vertex v;

  /**
   * Default constructor
   * @param transformation    String concatenation function
   */
  public OplusVertex(Function<Tuple2<String, String>, String> transformation) {
    super(transformation);
    v = new Vertex();
  }

  @Override
  public Vertex supplyEmpty() {

    v.setId(GradoopId.get());
    return v;
  }

}
