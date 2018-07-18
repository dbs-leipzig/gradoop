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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;

/**
 * (id, [candidates]) -> (id)
 *
 * Forwarded fields:
 *
 * f0: vertex id
 *
 * Read fields:
 *
 * f0: vertex id
 *
 * @param <K> key type
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f0")
public class BuildVertexStep<K> implements MapFunction<IdWithCandidates<K>, VertexStep<K>> {
  /**
   * Reduce instantiations
   */
  private final VertexStep<K> reuseTuple;

  /**
   * Constructor
   */
  public BuildVertexStep() {
    reuseTuple = new VertexStep<>();
  }

  @Override
  public VertexStep<K> map(IdWithCandidates<K> v) throws Exception {
    reuseTuple.setVertexId(v.getId());
    return reuseTuple;
  }
}
