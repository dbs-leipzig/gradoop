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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.io.Serializable;

/**
 * From a tuple (representing the outgoing edge from a vertex), extracts the target id (that is
 * going to be joined with its outgoing vertex)
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
@FunctionAnnotation.ReadFields("f1")
public class KeySelectorFromTupleProjetionWithTargetId implements KeySelector<Tuple2<Vertex, Edge>,GradoopId>, Serializable {
  @Override
  public GradoopId getKey(Tuple2<Vertex, Edge> value) throws Exception {
    return value.f1.getTargetId();
  }
}
