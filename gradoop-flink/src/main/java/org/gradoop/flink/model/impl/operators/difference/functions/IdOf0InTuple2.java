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
package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the identifier of first element in a tuple 2.
 *
 * @param <GD> graph data type
 * @param <C>  type of second element in tuple
 */
@FunctionAnnotation.ForwardedFields("f0.id->*")
public class IdOf0InTuple2<GD extends GraphHead, C>
  implements KeySelector<Tuple2<GD, C>, GradoopId> {

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getKey(Tuple2<GD, C> pair) throws Exception {
    return pair.f0.getId();
  }
}
