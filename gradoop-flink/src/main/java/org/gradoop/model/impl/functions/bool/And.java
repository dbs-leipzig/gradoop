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

package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Logical and as Flink function
 */
public class And implements CrossFunction<Boolean, Boolean, Boolean>,
  ReduceFunction<Boolean> {

  @Override
  public Boolean cross(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  @Override
  public Boolean reduce(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  public static DataSet<Boolean> union(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.union(b).reduce(new And());
  }

  public static DataSet<Boolean> cross(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.cross(b).with(new And());
  }
}
