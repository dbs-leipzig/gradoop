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
package org.gradoop.temporal.model.impl.operators.tostring;

import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.temporal.model.impl.operators.tostring.functions.TemporalElementToDataString;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

/**
 * Represents a temporal graph head by a data string (label, properties and valid time).
 *
 * @param <G> temporal graph head type
 */
public class TemporalGraphHeadToDataString<G extends TemporalGraphHead> extends TemporalElementToDataString<G>
  implements GraphHeadToString<G> {

  @Override
  public GraphHeadString map(G graph) {
    return new GraphHeadString(graph.getId(), "|" + labelWithProperties(graph) + time(graph) + "|");
  }
}
