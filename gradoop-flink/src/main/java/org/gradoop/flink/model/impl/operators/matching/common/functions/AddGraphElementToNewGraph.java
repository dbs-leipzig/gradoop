/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
