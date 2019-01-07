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
package org.gradoop.dataintegration.transformation.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * This {@link FlatMapFunction} extracts a {@link PropertyValue} and the origin Id of a vertex into
 * a Tuple.
 */
public class ExtractPropertyWithOriginId implements FlatMapFunction<Vertex, Tuple2<PropertyValue, GradoopId>> {

  /**
   * The property key of the property value.
   */
  private final String originalPropertyName;

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<PropertyValue, GradoopId> reuseTuple;

  /**
   * The constructor for extracting property value and its origin id.
   *
   * @param originalPropertyName The property key of the property value.
   */
  public ExtractPropertyWithOriginId(String originalPropertyName) {
    this.originalPropertyName = originalPropertyName;
    this.reuseTuple = new Tuple2<>();
  }

  @Override
  public void flatMap(Vertex vertex, Collector<Tuple2<PropertyValue, GradoopId>> out) {
    if (vertex.getProperties() != null &&
      vertex.getProperties().containsKey(originalPropertyName)) {
      PropertyValue pv = vertex.getPropertyValue(originalPropertyName);
      reuseTuple.f1 = vertex.getId();

      if (pv.isList()) {
        for (PropertyValue value : pv.getList()) {
          reuseTuple.f0 = value;
          out.collect(reuseTuple);
        }
      } else {
        reuseTuple.f0 = pv;
        out.collect(reuseTuple);
      }
    }
  }
}
