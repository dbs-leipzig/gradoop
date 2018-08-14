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

import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;
import org.gradoop.flink.model.impl.operators.drilling.functions.transformations.DrillDownTransformation;

/**
 * Creates a graph with the same structure but a specified property of an element is drilled down
 * by the declared function. The drilled down value can be stored under a new key. If the
 * original key shall be reused the old value is overwritten. Drill down can also be used
 * without specifying a drill down function when it is preceded by a roll up operation on the
 * same property key.
 */
public class DrillDown extends Drill {

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
  public DrillDown(
    String label, String propertyKey, DrillFunction vertexDrillFunction,
    DrillFunction edgeDrillFunction, DrillFunction graphheadDrillFunction,
    String newPropertyKey, Element element) {
    super(label, propertyKey, vertexDrillFunction, edgeDrillFunction,
      graphheadDrillFunction, newPropertyKey, element);
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    if (getElement() == Element.VERTICES) {
      graph = graph.transformVertices(
        new DrillDownTransformation<>(getLabel(), getPropertyKey(), getVertexDrillFunction(),
          getNewPropertyKey(), drillAllLabels(), keepCurrentPropertyKey()));
    } else if (getElement() == Element.EDGES) {
      graph = graph.transformEdges(
        new DrillDownTransformation<>(getLabel(), getPropertyKey(), getEdgeDrillFunction(),
          getNewPropertyKey(), drillAllLabels(), keepCurrentPropertyKey()));
    } else {
      graph = graph.transformGraphHead(
        new DrillDownTransformation<>(getLabel(), getPropertyKey(),
          getGraphheadDrillFunction(), getNewPropertyKey(), drillAllLabels(),
          keepCurrentPropertyKey()));
    }
    return graph;
  }

  @Override
  public String getName() {
    return DrillDown.class.getName();
  }

}
