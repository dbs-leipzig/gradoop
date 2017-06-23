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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Maps an edge into a quad
 */
@FunctionAnnotation.ForwardedFields("f0 -> f0; f1 -> f1; f3 -> f3; f3 -> f2")
public class DoHexMatchTarget implements MapFunction<Hexaplet, Hexaplet> {
  @Override
  public Hexaplet map(Hexaplet r) throws Exception {
    r.updateToMatchWithTarget();
    return r;
  }
}
