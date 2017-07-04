/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Used to select the source vertex id of an edge.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("sourceId->*")
public class SourceId<E extends Edge>
  implements KeySelector<E, GradoopId>, MapFunction<E, GradoopId> {

  @Override
  public GradoopId getKey(E edge) throws Exception {
    return edge.getSourceId();
  }

  @Override
  public GradoopId map(E edge) throws Exception {
    return edge.getSourceId();
  }
}
