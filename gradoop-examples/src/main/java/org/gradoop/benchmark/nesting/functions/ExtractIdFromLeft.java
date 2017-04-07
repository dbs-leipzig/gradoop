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

package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 07/04/17.
 */
@FunctionAnnotation.ForwardedFields("f0.f0 -> f0; f1 -> f1")
public class ExtractIdFromLeft
  implements MapFunction<Tuple2<ImportEdge<String>, GradoopId>, Tuple2<String, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<String, GradoopId> reusable;

  /**
   * Default constructor
   */
  public ExtractIdFromLeft() {
    reusable = new Tuple2<>();
  }

  @Override
  public Tuple2<String, GradoopId> map(
    Tuple2<ImportEdge<String>, GradoopId> importEdgeGradoopIdTuple2) throws Exception {
    reusable.f0 = importEdgeGradoopIdTuple2.f0.getId();
    reusable.f1 = importEdgeGradoopIdTuple2.f1;
    return reusable;
  }


}
