/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * join functions to update the source vertex string representation of an
 * edge string representation
 */
public class SourceStringUpdater
  implements JoinFunction<EdgeString, VertexString, EdgeString> {

  @Override
  public EdgeString join(
    EdgeString edgeString, VertexString sourceLabel) throws Exception {

    edgeString.setSourceLabel(sourceLabel.getLabel());

    return edgeString;
  }
}
