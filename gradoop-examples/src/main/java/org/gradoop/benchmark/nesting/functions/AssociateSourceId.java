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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 08/04/17.
 */
//@FunctionAnnotation.ForwardedFieldsFirst("f0.f0 -> f0; f1 -> f5; f0.f2 -> f2; f0.f3 -> f3;
// f0.f4 -> f4")
//@FunctionAnnotation.ForwardedFieldsSecond("f1.id -> f1")
public class AssociateSourceId<K extends Comparable<K>> implements JoinFunction<Tuple2<ImportEdge<K>,
GradoopId>, Tuple2<K, Vertex>, Tuple6<K, GradoopId, K,
    String,
    Properties, GradoopId>> {

  private final Tuple6<K, GradoopId, K, String, Properties, GradoopId> reusable = new
    Tuple6<>();

  @Override
  public Tuple6<K, GradoopId, K, String, Properties, GradoopId> join(Tuple2<ImportEdge<K>, GradoopId>
    first,
    Tuple2<K, Vertex> second) throws Exception {
    reusable.f0 = first.f0.f0;
    reusable.f1 = second.f1.getId();
    reusable.f2 = first.f0.f2;
    reusable.f3 = first.f0.f3;
    reusable.f4 = first.f0.f4;
    reusable.f5 = first.f1;
    return reusable;
  }

}
