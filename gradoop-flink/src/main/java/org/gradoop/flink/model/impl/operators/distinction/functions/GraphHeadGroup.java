/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.distinction.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * {@code (label, graphId) |><| graphHead => (label, graphHead)}
 *
 * @param <G> graph head type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")
public class GraphHeadGroup<G extends GraphHead>
  implements JoinFunction<GraphHeadString, G, Tuple2<String, G>> {

  @Override
  public Tuple2<String, G> join(GraphHeadString graphHeadString, G graphHead) throws Exception {
    return new Tuple2<>(graphHeadString.getLabel(), graphHead);
  }
}
