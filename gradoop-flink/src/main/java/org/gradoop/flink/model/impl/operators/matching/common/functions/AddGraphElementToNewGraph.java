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
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;

import java.util.HashMap;

/**
 * (GE) -> (GE (+ GraphHead), GraphHead)
 *
 * Forwarded fields:
 *
 * *->f0: input graph element
 *
 * @param <GE> EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class AddGraphElementToNewGraph<GE extends GraphElement>
  implements MapFunction<GE, Tuple2<GE, GraphHead>> {
  /**
   * EPGM graph head factory
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;
  /**
   * Variable assigned to the query vertex
   */
  private final String variable;
  /**
   * Reduce instantiations
   */
  private final Tuple2<GE, GraphHead> reuseTuple;
  /**
   * Reuse map for variable mapping
   */
  private final HashMap<PropertyValue, PropertyValue> reuseVariableMapping;

  /**
   * Constructor
   *
   * @param graphHeadFactory EPGM graph head factory
   * @param variable Variable assigned to the only query vertex
   */
  public AddGraphElementToNewGraph(EPGMGraphHeadFactory<GraphHead> graphHeadFactory,
    String variable) {
    this.graphHeadFactory = graphHeadFactory;
    this.variable = variable;
    reuseTuple = new Tuple2<>();
    reuseVariableMapping = new HashMap<>();
  }

  @Override
  public Tuple2<GE, GraphHead> map(GE value) throws Exception {
    reuseVariableMapping.clear();
    reuseVariableMapping.put(
      PropertyValue.create(this.variable),
      PropertyValue.create(value.getId())
    );

    GraphHead graphHead = graphHeadFactory.createGraphHead();
    graphHead.setProperty(PatternMatching.VARIABLE_MAPPING_KEY, reuseVariableMapping);

    value.addGraphId(graphHead.getId());

    reuseTuple.f0 = value;
    reuseTuple.f1 = graphHead;
    return reuseTuple;
  }
}
