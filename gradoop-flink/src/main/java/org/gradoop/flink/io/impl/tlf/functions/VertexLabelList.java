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
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Collects all vertex labels.
 */
public class VertexLabelList
  implements FlatMapFunction<GraphTransaction, String> {

  @Override
  public void flatMap(GraphTransaction graphTransaction, Collector<String> collector)
    throws Exception {
    for (Vertex vertex : graphTransaction.getVertices()) {
      collector.collect(vertex.getLabel());
    }
  }
}
