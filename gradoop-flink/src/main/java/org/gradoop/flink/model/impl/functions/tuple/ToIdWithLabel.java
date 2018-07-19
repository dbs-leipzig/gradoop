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
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;

/**
 * Factory to create (id, label) pairs from EPGM elements.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->f0;label->f1")
public class ToIdWithLabel<EL extends Element> implements MapFunction<EL, IdWithLabel> {
  /**
   * Reuse tuple
   */
  private final IdWithLabel reuseTuple = new IdWithLabel();

  @Override
  public IdWithLabel map(EL element) {
    reuseTuple.setId(element.getId());
    reuseTuple.setLabel(element.getLabel());
    return reuseTuple;
  }
}
