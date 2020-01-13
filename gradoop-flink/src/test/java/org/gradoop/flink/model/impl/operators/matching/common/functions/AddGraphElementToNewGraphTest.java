/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AddGraphElementToNewGraphTest {
  @Test
  public void testVariableMappingCreation() throws Exception {
    AddGraphElementToNewGraph<EPGMVertex, EPGMGraphHead> udf =
      new AddGraphElementToNewGraph<>(new EPGMGraphHeadFactory(), "a");

    EPGMVertex vertex = new EPGMVertexFactory().createVertex();
    Tuple2<EPGMVertex, EPGMGraphHead> result = udf.map(vertex);

    assertTrue(result.f1.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));
    Map<PropertyValue, PropertyValue> variableMapping =
      result.f1.getPropertyValue(PatternMatching.VARIABLE_MAPPING_KEY).getMap();

    assertEquals(vertex.getId(), variableMapping.get(PropertyValue.create("a")).getGradoopId());
  }
}
