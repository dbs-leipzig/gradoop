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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * The {@link JoinFunction} builds new {@link GradoopId} / {@link PropertyValue} pairs.
 * This function is used to propagate a property along an edge.
 *
 * @param <E> The edge type.
 */
public class BuildTargetVertexIdPropertyValuePairs<E extends EPGMEdge>
  implements JoinFunction<Tuple2<GradoopId, PropertyValue>, E, Tuple2<GradoopId, PropertyValue>> {

  @Override
  public Tuple2<GradoopId, PropertyValue> join(Tuple2<GradoopId, PropertyValue> t, E e) {
    t.f0 = e.getTargetId();
    return t;
  }
}
