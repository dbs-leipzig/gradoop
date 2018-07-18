/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Join an edge tuple with a tuple containing the source vertex id of this edge
 * and its new graphs.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f2->f2;f3->f3")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class JoinWithSourceGraphIdSet
  implements JoinFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdSet>,
  Tuple2<GradoopId, GradoopIdSet>,
  Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet> reuseTuple
    = new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet> join(
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdSet> edge,
    Tuple2<GradoopId, GradoopIdSet> vertex) throws
    Exception {
    reuseTuple.f0 = edge.f0;
    reuseTuple.f1 = vertex.f1;
    reuseTuple.f2 = edge.f2;
    reuseTuple.f3 = edge.f3;
    return reuseTuple;
  }
}
