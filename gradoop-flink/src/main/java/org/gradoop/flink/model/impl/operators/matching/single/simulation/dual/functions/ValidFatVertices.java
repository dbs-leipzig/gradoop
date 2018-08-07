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
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;

/**
 * Filters a {@link FatVertex} if it has query candidates.
 *
 * Read fields:
 *
 * f1: vertex query candidates
 *
 */
@FunctionAnnotation.ReadFields("f1")
public class ValidFatVertices implements FilterFunction<FatVertex> {

  @Override
  public boolean filter(FatVertex fatVertex) throws Exception {
    return fatVertex.getCandidates().size() > 0;
  }
}
