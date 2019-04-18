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
package org.gradoop.flink.model.impl.operators.cypher.capf.query.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Method to filter edge tuples by their labels.
 */
@FunctionAnnotation.ReadFields("f3")
public class EdgeLabelFilter
  implements FilterFunction<Tuple5<Long, Long, Long, String, Properties>> {

  /**
   * The label to be filtered for.
   */
  private String label;

  /**
   * Constructor.
   *
   * @param label label to be filtered for
   */
  public EdgeLabelFilter(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(Tuple5<Long, Long, Long, String, Properties> tuple) throws Exception {
    return tuple.f3.equals(label);
  }
}
