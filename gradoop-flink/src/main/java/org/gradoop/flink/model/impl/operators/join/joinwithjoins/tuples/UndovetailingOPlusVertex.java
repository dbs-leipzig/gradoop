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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;

import java.io.Serializable;

/**
 * Utility semantic tuple associating each newly created vertex to the ids
 * of the former graph operads
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public class UndovetailingOPlusVertex extends
  Tuple5<Boolean,GradoopId,Boolean,GradoopId, Vertex> implements Serializable {

  /**
   * Default constructor
   * @param leftB                      If the left value is significant or not
   * @param leftId                     The actual left id
   * @param rightB                      If the right value is significant or not
   * @param rightId                     The actual right id
   * @param mergedCorrespondingVertex  Newly created vertex from left and right
   */
  public UndovetailingOPlusVertex(Boolean leftB, GradoopId leftId,
    Boolean rightB, GradoopId rightId,
    Vertex mergedCorrespondingVertex) {
    super(leftB,leftId,rightB,rightId,mergedCorrespondingVertex);
  }

  /**
   * Required element-free constructor, (otherwise Apache Flink curses…)
   */
  public UndovetailingOPlusVertex() {
    super();
  }
}
