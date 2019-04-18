/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.cypher.capf.result.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Adds GradoopIds to the graph ids of a GraphElement based on a long id
 * that is defined on both datasets.
 *
 * @param <E> a type extending GraphElement, e.g. Vertex or Edge
 */

@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class AddNewGraphs<E extends GraphElement>
  implements JoinFunction<Tuple2<Long, GradoopIdSet>, Tuple2<Long, E>, E> {

  @Override
  public E join(
    Tuple2<Long, GradoopIdSet> idTuple,
    Tuple2<Long, E> elementTuple) throws Exception {

    E element = elementTuple.f1;
    for (GradoopId id : idTuple.f1) {
      element.addGraphId(id);
    }
    return element;
  }
}
