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

package org.gradoop.flink.model.impl.operators.drilling;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;
import org.gradoop.flink.model.impl.operators.drilling.functions.transformations.DrillDownTransformation;

/**
 * Creates a graph with the same structure but a specified property of an element is drilled down
 * by the declared function. It is possible to drill down either on one vertex / edge type or all
 * vertex / edge types. Additionally the drilled down value can be stored under a new key. If the
 * original key shall be reused the old value is overwritten. Drill down can also be used
 * without specifying a drill down function when it is preceded by a roll up operation on the
 * same property key.
 */
public class DrillDown extends Drill {

  /**
   * Valued constructor.
   *
   * @param label          label of the element whose property shall be drilled, or
   *                       see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey    property key
   * @param function       drill function which shall be applied to a property
   * @param newPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   * @param drillVertex    true, if vertices shall be drilled, false for edges
   */
  public DrillDown(
    String label, String propertyKey, DrillFunction function, String newPropertyKey,
    boolean drillVertex) {
    super(label, propertyKey, function, newPropertyKey, drillVertex);
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    if (isDrillVertex()) {
      graph = graph.transformVertices(
        new DrillDownTransformation<Vertex>(getLabel(), getPropertyKey(), getFunction(),
          getNewPropertyKey()));
    } else {
      graph = graph.transformEdges(
        new DrillDownTransformation<Edge>(getLabel(), getPropertyKey(), getFunction(),
          getNewPropertyKey()));
    }
    return graph;
  }

  @Override
  public String getName() {
    return DrillDown.class.getName();
  }

}
