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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Join the new GradoopIds, representing the new graphs, with the vertices by
 * adding them to the vertices graph sets
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1")
public class JoinVertexIdWithGraphIds
  implements JoinFunction<Tuple2<GradoopId, PropertyValue>,
  Tuple2<PropertyValue, GradoopId>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, PropertyValue> vertexSplitKey,
      Tuple2<PropertyValue, GradoopId> splitKeyGradoopId) {
    reuseTuple.setFields(vertexSplitKey.f0, splitKeyGradoopId.f1);
    return reuseTuple;
  }
}
