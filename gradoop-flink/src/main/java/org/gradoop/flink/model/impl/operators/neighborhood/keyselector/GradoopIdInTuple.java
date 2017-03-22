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

package org.gradoop.flink.model.impl.operators.neighborhood.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class GradoopIdInTuple<T extends Tuple> implements KeySelector<T, GradoopId> {

  private boolean sourceId;
  private boolean targetId;
  private int[] fields;

  public GradoopIdInTuple(int... fields) {
    this(false, false, fields);
  }

  public GradoopIdInTuple(boolean sourceId, boolean targetId, int... fields) {
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.fields = fields;
  }

  //0
  @Override
  public GradoopId getKey(T t) throws Exception {

    Tuple tuple = (Tuple)t;
    if (fields.length != 1) {
      for (int i = 0; i < fields.length-1; i++) {
        tuple = (Tuple)tuple.getField(fields[i]);
      }
    }
    if (sourceId) {
      return ((Edge)tuple.getField(fields[fields.length - 1])).getSourceId();
    } else if (targetId) {
      return ((Edge)tuple.getField(fields[fields.length - 1])).getTargetId();
    } else {
      return ((EPGMGraphElement)tuple.getField(fields[fields.length - 1])).getId();
    }
  }
}
