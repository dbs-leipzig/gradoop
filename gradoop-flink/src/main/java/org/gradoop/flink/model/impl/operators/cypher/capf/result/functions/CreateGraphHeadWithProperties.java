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
package org.gradoop.flink.model.impl.operators.cypher.capf.result.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.List;

/**
 * Create a graph head from a row while automatically setting properties. The fields between start
 * and end are expected to be the properties in order of the property names, the last field of the
 * row is expected to be a {@link GradoopId}.
 */
public class CreateGraphHeadWithProperties implements MapFunction<Row, GraphHead> {

  /**
   * The start index of fields that will be set as properties of the graph heads.
   */
  private int start;

  /**
   * The end index of fields that will be set as properties of the graph heads.
   */
  private int end;

  /**
   * The factory used to create the graph heads.
   */
  private GraphHeadFactory headFactory;

  /**
   * List containing names for the properties to be set.
   */
  private List<String> propertyNames;

  /**
   * Constructor.
   *
   * @param start         start index of property fields
   * @param end           end index of property fields
   * @param headFactory   graph head factory
   * @param propertyNames names of the properties to be set
   */
  public CreateGraphHeadWithProperties(
    int start,
    int end,
    GraphHeadFactory headFactory,
    List<String> propertyNames) {

    this.start = start;
    this.end = end;
    this.headFactory = headFactory;
    this.propertyNames = propertyNames;
  }

  @Override
  public GraphHead map(Row row) {

    GraphHead head = headFactory.initGraphHead((GradoopId) row.getField(row.getArity() - 1));
    Properties props = new Properties();

    for (int i = start; i < end; i++) {
      props.set(propertyNames.get(i), row.getField(i));
    }

    head.setProperties(props);

    return head;
  }
}
