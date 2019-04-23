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
package org.gradoop.flink.model.impl.operators.cypher.capf.query.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;

/**
 * KeySelector that returns the id of a {@link Element} in the second field of a tuple.
 *
 * @param <E> any GraphElement
 */
@FunctionAnnotation.ForwardedFields("f1.id->*")
public class IdOfF1<E extends Element> implements KeySelector<Tuple2<Long, E>, GradoopId> {

  @Override
  public GradoopId getKey(Tuple2<Long, E> tuple) {
    return tuple.f1.getId();
  }
}
