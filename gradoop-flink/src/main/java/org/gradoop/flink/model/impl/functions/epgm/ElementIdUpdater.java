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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;


/**
 * Updates the id of an EPGM element in a Tuple2 by the GradoopId in the
 * second field.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFieldsFirst("graphIds;label;properties")
@FunctionAnnotation.ForwardedFieldsSecond("*->id")
public class ElementIdUpdater<EL extends Element>
  implements MapFunction<Tuple2<EL, GradoopId>, EL> {

  /**
   * {@inheritDoc}
   */
  @Override
  public EL map(Tuple2<EL, GradoopId> tuple2) {
    tuple2.f0.setId(tuple2.f1);
    return tuple2.f0;
  }
}
