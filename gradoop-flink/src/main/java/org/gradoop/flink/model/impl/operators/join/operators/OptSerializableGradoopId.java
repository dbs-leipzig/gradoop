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

package org.gradoop.flink.model.impl.operators.join.operators;

import org.gradoop.common.model.impl.id.GradoopId;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class OptSerializableGradoopId extends OptSerializable<GradoopId> implements
  Comparable<OptSerializableGradoopId> {

  public OptSerializableGradoopId(boolean isThereElement, GradoopId elem) {
    super(isThereElement, elem);
  }

  public OptSerializableGradoopId(GradoopId elem) {
    super(elem);
  }

  public static  OptSerializableGradoopId empty() {
    return new OptSerializableGradoopId(false,null);
  }

  public static OptSerializableGradoopId value(GradoopId val) {
    return new OptSerializableGradoopId(true,val);
  }

  @Override
  public int compareTo(OptSerializableGradoopId o) {
    if (o==null) return 1;
    if (isPresent()) {
      if (o.isPresent())
        return 0;
      else {
        return get().compareTo(o.get());
      }
    } else {
      if (o.isPresent())
        return -1;
      else
        return 0;
    }
  }
}
