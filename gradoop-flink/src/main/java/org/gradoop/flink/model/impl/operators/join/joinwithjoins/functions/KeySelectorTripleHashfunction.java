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
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

/**
 * Hashing the triples (vertices from the to-be-returned graph and the edge from one of the two
 * operands) by ignoring the aforementioned edge and hashing by vertices' id
 *
 * Created by vasistas on 14/02/17.
 */
@FunctionAnnotation.ReadFields("f0; f2")
public class KeySelectorTripleHashfunction implements
  KeySelector<Triple,Integer> {
  @Override
  public Integer getKey(Triple t) throws Exception {
    return t.f0.getId().hashCode() * 13 + t.f2.getId().hashCode();
  }
}
