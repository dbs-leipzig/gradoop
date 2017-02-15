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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

/**
 * Given a Tuple2 containing the source vertex and the edge, and another element, it constructs
 * the triple
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0; f1->f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class JoinFunctionCreateTriple implements
  JoinFunction<Tuple2<Vertex, Edge>, Tuple2<GradoopId, Vertex>, Triple> {
  @Override
  public Triple join(Tuple2<Vertex, Edge> first,
    Tuple2<GradoopId, Vertex> second) throws Exception {
    return new Triple(first.f0, first.f1, second.f1);
  }
}
