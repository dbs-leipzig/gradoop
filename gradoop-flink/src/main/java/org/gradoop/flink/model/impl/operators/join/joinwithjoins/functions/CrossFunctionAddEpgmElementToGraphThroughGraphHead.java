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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * This function adds the GraphElement to the given graph head (the resulting graph)
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
@FunctionAnnotation.ReadFieldsSecond("id")
public class CrossFunctionAddEpgmElementToGraphThroughGraphHead<K extends GraphElement> implements CrossFunction<K, GraphHead,
  K> {
  @Override
  public K cross(K val1, GraphHead val2) throws Exception {
    val1.addGraphId(val2.getId());
    return val1;
  }
}
