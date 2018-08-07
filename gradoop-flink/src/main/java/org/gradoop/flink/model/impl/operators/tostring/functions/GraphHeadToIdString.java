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

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * represents a graph head by an id string
 */
public class GraphHeadToIdString implements GraphHeadToString<GraphHead> {

  @Override
  public GraphHeadString map(GraphHead graphHead) throws Exception {
    return new GraphHeadString(graphHead.getId(), graphHead.getId().toString());
  }
}
