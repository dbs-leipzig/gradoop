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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Extracts the source id from a tuple in the iterative set.
 */
public class GetSourceIdFromIterativeTuple implements MapFunction<IterativeTuple, GradoopId> {

  @Override
  public GradoopId map(IterativeTuple iterativeTuple) throws Exception {
    return iterativeTuple.getSourceId();
  }
}
