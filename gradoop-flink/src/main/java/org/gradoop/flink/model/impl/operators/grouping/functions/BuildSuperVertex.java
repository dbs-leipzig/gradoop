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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;


import java.util.List;

/**
 * Creates a new super vertex representing a vertex group. The vertex stores the
 * group label, the group property value and the aggregate values for its group.
 */
@FunctionAnnotation.ForwardedFields("f1->id")
@FunctionAnnotation.ReadFields("f1;f2;f3;f4")
public class BuildSuperVertex extends BuildBase implements
  MapFunction<VertexGroupItem, Vertex>, ResultTypeQueryable<Vertex> {

  /**
   * Vertex vertexFactory.
   */
  private final VertexFactory vertexFactory;

  /**
   * Creates map function.
   *
   * @param groupPropertyKeys vertex property key for grouping
   * @param useLabel          true, if vertex label shall be considered
   * @param valueAggregators  aggregate functions for vertex values
   * @param vertexFactory     vertex factory
   */
  public BuildSuperVertex(List<String> groupPropertyKeys,
    boolean useLabel,
    List<PropertyValueAggregator> valueAggregators,
    VertexFactory vertexFactory) {
    super(groupPropertyKeys, useLabel, valueAggregators);
    this.vertexFactory = vertexFactory;
  }

  /**
   * Creates a {@link Vertex} object from the given {@link
   * VertexGroupItem} and returns a new {@link org.apache.flink.graph.Vertex}.
   *
   * @param groupItem vertex group item
   * @return vertex including new vertex data
   * @throws Exception
   */
  @Override
  public Vertex map(VertexGroupItem groupItem) throws
    Exception {
    Vertex supVertex = vertexFactory.initVertex(groupItem.getSuperVertexId());

    setLabel(supVertex, groupItem.getGroupLabel());
    setGroupProperties(supVertex, groupItem.getGroupingValues());
    setAggregateValues(supVertex, groupItem.getAggregateValues());

    return supVertex;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<Vertex> getProducedType() {
    return (TypeInformation<Vertex>) TypeExtractor
      .createTypeInfo(vertexFactory.getType());
  }
}
