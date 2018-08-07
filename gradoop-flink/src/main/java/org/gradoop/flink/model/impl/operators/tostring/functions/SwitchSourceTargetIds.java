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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * switches source and target id of an edge string representation
 */
public class SwitchSourceTargetIds implements
  MapFunction<EdgeString, EdgeString> {

  @Override
  public EdgeString map(EdgeString edgeString) throws Exception {

    GradoopId sourceId = edgeString.getSourceId();
    edgeString.setSourceId(edgeString.getTargetId());
    edgeString.setTargetId(sourceId);

    return edgeString;
  }
}
