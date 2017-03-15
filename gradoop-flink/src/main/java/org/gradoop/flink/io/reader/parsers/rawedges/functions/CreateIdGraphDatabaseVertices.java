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

package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Associate to each vertex id its graph id
 * @param <Element> The element that will be associated to an unique id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1 -> f1")
public class CreateIdGraphDatabaseVertices<Element> implements
  JoinFunction<Tuple2<GradoopId, Element>,
               Tuple3<Element, GradoopId, Vertex>,
               Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Default constructor
   */
  public CreateIdGraphDatabaseVertices() {
    reusable = new Tuple2<>();
  }

  @Override
  public Tuple2<GradoopId, GradoopId> join(Tuple2<GradoopId, Element> first,
    Tuple3<Element, GradoopId, Vertex> second) throws Exception {
    reusable.f0 = first.f0;
    reusable.f1 = second.f1;
    return reusable;
  }

}
