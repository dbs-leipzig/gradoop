package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;
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

public class AddGraphElementToNewGraphTest {
  @Test
  public void testVariableMappingCreation() throws Exception {
    AddGraphElementToNewGraph<Vertex> udf =
      new AddGraphElementToNewGraph<>(new GraphHeadFactory(), "a");

    Vertex vertex = new VertexFactory().createVertex();
    Tuple2<Vertex, GraphHead> result = udf.map(vertex);

    assertTrue(result.f1.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));
    Map<PropertyValue, PropertyValue> variableMapping =
      result.f1.getPropertyValue(PatternMatching.VARIABLE_MAPPING_KEY).getMap();

    assertEquals(vertex.getId(), variableMapping.get(PropertyValue.create("a")).getGradoopId());
  }
}