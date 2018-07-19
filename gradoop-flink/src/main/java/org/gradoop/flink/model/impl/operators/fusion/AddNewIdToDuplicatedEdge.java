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
package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * It can be possible to reuse an edge. If a group contains an edge only once, we keep it as is.
 * If it contains duplicates, we keep the first instance but replace the id for all duplicates with
 * fresh ids.
 */
public class AddNewIdToDuplicatedEdge implements GroupReduceFunction<Edge, Edge> {

  /* (non-Javadoc)
   * @see org.apache.flink.api.common.functions.GroupReduceFunction#reduce(java.lang.Iterable, org.apache.flink.util.Collector)
   */
  @Override
  public void reduce(Iterable<Edge> values, Collector<Edge> out) throws Exception {
    boolean sawFirst = false;
    for (Edge edge: values) {
      if (sawFirst) {
        edge.setId(GradoopId.get());
      } else {
        sawFirst = true;
      }
      out.collect(edge);
    }
  }

}
