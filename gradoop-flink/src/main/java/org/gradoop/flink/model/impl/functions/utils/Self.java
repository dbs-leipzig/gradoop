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

package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Uses the GradoopId element itself as a selection criterion
 * @param <K> any type
 */
@FunctionAnnotation.ForwardedFields("* -> *")
public class Self<K> implements KeySelector<K, K>, MapFunction<K, K> {
  @Override
  public K getKey(K value) throws Exception {
    return value;
  }

  @Override
  public K map(K value) throws Exception {
    return value;
  }
}
