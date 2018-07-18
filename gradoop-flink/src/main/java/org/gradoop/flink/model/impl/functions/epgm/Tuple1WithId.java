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
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * element => (elementId)
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class Tuple1WithId<EL extends Element>
  implements MapFunction<EL, Tuple1<GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple1<GradoopId> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<GradoopId> map(EL element) throws Exception {
    reuseTuple.f0 = element.getId();
    return reuseTuple;
  }
}
