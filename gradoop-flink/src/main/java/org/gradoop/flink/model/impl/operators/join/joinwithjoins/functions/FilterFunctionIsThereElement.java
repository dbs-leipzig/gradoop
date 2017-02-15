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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.UndovetailingOPlusVertex;

/**
 * This function is used to filter only those vertices that appear in the respective operand
 * (<code>isLeft</code>) and that have been selected in the intermediate vertex join operation
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
@FunctionAnnotation.NonForwardedFields("f0; f1")
public class FilterFunctionIsThereElement implements FilterFunction<UndovetailingOPlusVertex> {

  public final boolean isLeft;

  public FilterFunctionIsThereElement(boolean isLeft) {
    this.isLeft = isLeft;
  }

  @Override
  public boolean filter(UndovetailingOPlusVertex value) throws Exception {
    return isLeft ? value.f0.isThereElement : value.f1.isThereElement;
  }
}
