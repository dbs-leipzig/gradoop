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
package org.gradoop.flink.io.impl.minimal.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * (edgeId, (sourceId, targetId), label, properties) => ImportEdge
 *
 * Forwarded fields:
 *
 * f0:        edgeId
 * f1.f0->f1: sourceId
 * f1.f1->f2: targetId
 * f2:        edgeLabel
 * f3:        edgeProperties
 *
 * @param <K> id type
 */
public class CreateLabeledImportEdgeProperties<K extends Comparable<K>>
implements MapFunction<Tuple4<K, Tuple2<K, K>, String, Properties>, ImportEdge<K>>  {

  /**
  * Used ImportEdge
  */
  private ImportEdge edge;

  /**
  * Constructor
  */
  public CreateLabeledImportEdgeProperties() {

    this.edge = new ImportEdge<>();
  }

  @Override
  public ImportEdge<K> map(Tuple4<K, Tuple2<K, K>, String, Properties> value) throws Exception {

    edge.setId(value.f0);
    edge.setSourceId(value.f1.f0);
    edge.setTargetId(value.f1.f1);
    edge.setLabel(value.f2);
    edge.setProperties(value.f3);
    return edge;
  }
}
