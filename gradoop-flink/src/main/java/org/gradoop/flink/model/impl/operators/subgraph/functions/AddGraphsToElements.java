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
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Add all gradoop ids in the second field of the first tuple to the element.
 * id:el{id1} join (id, {id2, id3}) -> id:el{id1, id2, id3}
 * @param <EL> epgm graph element type
 */
@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;properties")
public class AddGraphsToElements<EL extends GraphElement>
  implements JoinFunction<Tuple2<GradoopId, GradoopIdSet>, EL, EL> {

  @Override
  public EL join(
    Tuple2<GradoopId, GradoopIdSet> left,
    EL right) {
    right.getGraphIds().addAll(left.f1);
    return right;
  }
}
