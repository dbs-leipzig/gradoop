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
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;

/**
 * (edge) -> (sourceId, label)
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;label->f1")
public class ToSourceIdWithLabel<E extends Edge> implements MapFunction<E, IdWithLabel> {
  /**
   * Reuse tuple
   */
  private final IdWithLabel reuseTuple = new IdWithLabel();

  @Override
  public IdWithLabel map(E edge) throws Exception {
    reuseTuple.setId(edge.getSourceId());
    reuseTuple.setLabel(edge.getLabel());
    return reuseTuple;
  }
}
