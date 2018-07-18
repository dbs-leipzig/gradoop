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
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * CoGroups tuples containing gradoop ids and gradoop id sets with graph
 * elements with the same gradoop ids and adds the gradoop id sets to each
 * element.
 * @param <EL> epgm graph element type
 */

@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;properties")
public class AddGraphsToElementsCoGroup<EL extends GraphElement>
  implements CoGroupFunction<Tuple2<GradoopId, GradoopIdSet>, EL, EL> {

  @Override
  public void coGroup(
    Iterable<Tuple2<GradoopId, GradoopIdSet>> graphs,
    Iterable<EL> elements,
    Collector<EL> collector) throws Exception {
    boolean wasGraphSetEmpty = true;
    for (EL element : elements) {
      for (Tuple2<GradoopId, GradoopIdSet> graphSet : graphs) {
        element.getGraphIds().addAll(graphSet.f1);
        wasGraphSetEmpty = false;
      }
      if (!wasGraphSetEmpty) {
        collector.collect(element);
      }
    }
  }
}
