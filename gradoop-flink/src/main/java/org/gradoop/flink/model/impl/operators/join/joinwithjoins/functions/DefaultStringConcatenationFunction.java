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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.functions.Function;

/**
 * This function defines a default way to concatenate two strings, that is simply append them
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
@FunctionAnnotation.NonForwardedFields("f0; f1")
public class DefaultStringConcatenationFunction implements Function<Tuple2<String,String>,String> {
  @Override
  public String apply(Tuple2<String, String> entity) {
    return entity.f0+entity.f1;
  }
}
