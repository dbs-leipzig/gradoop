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
package org.gradoop.flink.model.impl.operators.drilling;

import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;
import org.gradoop.flink.model.impl.operators.drilling.functions.transformations.RollUpTransformation;

/**
 * Creates a graph with the same structure but a specified property of an element is drilled up
 * by the declared function. The drilled up value can be stored under a new key. If the
 * original key shall be reused the old value is stored under the key 'key__x' where 'x' is a
 * version number. This number increases on every continuous drill up call where the highest
 * number is the level direct below the drilled up one.
 * <p>
 * Consider the following example:
 * <p>
 * Input vertices:<br>
 * (0, "Person", {yob: 1973})<br>
 * (1, "Person", {yob: 1977})<br>
 * (2, "Person", {yob: 1984})<br>
 * (3, "Person", {yob: 1989})<br>
 * <p>
 * Output vertices (drilled by the century of their year of birth):<br>
 * (0, "Person", {yob: 1970, yob__1: 1973})<br>
 * (1, "Person", {yob: 1970, yob__1: 1977})<br>
 * (2, "Person", {yob: 1980, yob__1: 1984})<br>
 * (3, "Person", {yob: 1980, yob__1: 1989})<br>
 * <p>
 * This example shows that this operation may be used as pre processing for the
 * {@link org.gradoop.flink.model.impl.operators.grouping.Grouping} operator.
 */
public class RollUp extends Drill {

  /**
   * Valued constructor.
   *
   * @param label                   label of the element whose property shall be drilled
   * @param propertyKey             property key
   * @param vertexDrillFunction     drill function which shall be applied to a property of a vertex
   * @param edgeDrillFunction       drill function which shall be applied to a property of an edge
   * @param graphheadDrillFunction  drill function which shall be applied to a property of a
   *                                graph head
   * @param newPropertyKey          new property key
   * @param element                 Element to be covered by the operation
   */
  RollUp(String label, String propertyKey, DrillFunction vertexDrillFunction,
  DrillFunction edgeDrillFunction, DrillFunction graphheadDrillFunction,
  String newPropertyKey, Element element) {
    super(label, propertyKey, vertexDrillFunction, edgeDrillFunction, graphheadDrillFunction,
    newPropertyKey, element);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
  	switch (getElement()) {
    case VERTICES:
      graph = graph.transformVertices(
        new RollUpTransformation<>(
          getLabel(),
          getPropertyKey(),
          getVertexDrillFunction(),
          getNewPropertyKey(),
          drillAllLabels(),
          keepCurrentPropertyKey()));
      break;
    case EDGES:
      graph = graph.transformEdges(
        new RollUpTransformation<>(
          getLabel(),
          getPropertyKey(),
          getEdgeDrillFunction(),
          getNewPropertyKey(),
          drillAllLabels(),
          keepCurrentPropertyKey()));
      break;
    case GRAPHHEAD:
      graph = graph.transformGraphHead(
        new RollUpTransformation<>(
          getLabel(),
          getPropertyKey(),
          getGraphheadDrillFunction(),
          getNewPropertyKey(),
          drillAllLabels(),
          keepCurrentPropertyKey()));
      break;
      default: throw new UnsupportedTypeException("Element type must be vertex, edge or graphhead");
  	}
    return graph;
  }

  @Override
  public String getName() {
    return RollUp.class.getName();
  }
}
