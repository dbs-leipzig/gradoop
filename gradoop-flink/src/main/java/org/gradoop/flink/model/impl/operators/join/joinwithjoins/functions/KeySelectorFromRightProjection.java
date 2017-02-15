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

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;

import java.io.Serializable;

/**
 * Defines a KeySelector from the second projection of a tuple
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
@FunctionAnnotation.ReadFields("f1")
public class KeySelectorFromRightProjection implements KeySelector<Tuple2<Vertex,
  OptSerializableGradoopId>, Integer>,
  Serializable {

  /**
   * Default constructor
   */
  public KeySelectorFromRightProjection() {  }

  @Override
  public Integer getKey(Tuple2<Vertex, OptSerializableGradoopId> value) throws Exception {
    return value.f1.hashCode();
  }
}
