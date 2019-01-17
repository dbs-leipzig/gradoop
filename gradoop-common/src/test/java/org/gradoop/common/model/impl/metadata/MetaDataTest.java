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
package org.gradoop.common.model.impl.metadata;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class MetaDataTest {

  private static final List<PropertyValue> propValues = new ArrayList<>();
  private static final List<Class<?>> propTypes = new ArrayList<>();

  static {
    propValues.add(PropertyValue.create(null));
    propValues.add(PropertyValue.create(true));
    propValues.add(PropertyValue.create(1));
    propValues.add(PropertyValue.create(1L));
    propValues.add(PropertyValue.create(1.0f));
    propValues.add(PropertyValue.create(1.0));
    propValues.add(PropertyValue.create(""));
    propValues.add(PropertyValue.create(new BigDecimal(1.0)));
    propValues.add(PropertyValue.create(GradoopId.get()));
    Map<PropertyValue, PropertyValue> testMap = new HashMap<>();
    testMap.put(PropertyValue.create("testKey"), PropertyValue.create("testValue"));
    propValues.add(PropertyValue.create(testMap));
    propValues.add(PropertyValue.create(
      Collections.singletonList(PropertyValue.create("testEntry"))));
    propValues.add(PropertyValue.create(LocalDate.now()));
    propValues.add(PropertyValue.create(LocalTime.now()));
    propValues.add(PropertyValue.create(LocalDateTime.now()));
    propValues.add(PropertyValue.create((short) 1));
    propValues.add(PropertyValue.create(Sets.newSet(PropertyValue.create("testEntry"))));

    propTypes.add(null);
    propTypes.add(Boolean.class);
    propTypes.add(Integer.class);
    propTypes.add(Long.class);
    propTypes.add(Float.class);
    propTypes.add(Double.class);
    propTypes.add(String.class);
    propTypes.add(BigDecimal.class);
    propTypes.add(GradoopId.class);
    propTypes.add(Map.class);
    propTypes.add(List.class);
    propTypes.add(LocalDate.class);
    propTypes.add(LocalTime.class);
    propTypes.add(LocalDateTime.class);
    propTypes.add(Short.class);
    propTypes.add(Set.class);
  }

  /**
   * Test the meta data maps for vertices, edges and graphs.
   */
  @Test
  public void testMetaDataMaps() {

    Map<String, List<PropertyMetaData>> graphMetaData = new HashMap<>();

    List<PropertyMetaData> gPropMetaData1 = Arrays.asList(
      new PropertyMetaData(
        "gKey1", MetaData.getTypeString(propValues.get(5)), null), // Double
      new PropertyMetaData(
        "gKey2", MetaData.getTypeString(propValues.get(8)), null) // GradoopId
    );
    graphMetaData.put("g1", gPropMetaData1);

    List<PropertyMetaData> gPropMetaData2 = Arrays.asList(
      new PropertyMetaData(
        "gKey3", MetaData.getTypeString(propValues.get(10)), null), // List
      new PropertyMetaData(
        "gKey4", MetaData.getTypeString(propValues.get(2)), null) // Integer
    );
    graphMetaData.put("g2", gPropMetaData2);

    Map<String, List<PropertyMetaData>> vertexMetaData = new HashMap<>();

    List<PropertyMetaData> vPropMetaData1 = Arrays.asList(
      new PropertyMetaData(
        "vKey1", MetaData.getTypeString(propValues.get(11)), null), // LocalDate
      new PropertyMetaData(
        "vKey2", MetaData.getTypeString(propValues.get(15)), null) // Set
    );
    vertexMetaData.put("v1", vPropMetaData1);

    List<PropertyMetaData> vPropMetaData2 = Arrays.asList(
      new PropertyMetaData(
        "vKey3", MetaData.getTypeString(propValues.get(0)), null), // null
      new PropertyMetaData(
        "vKey4", MetaData.getTypeString(propValues.get(1)), null) // Boolean
    );
    vertexMetaData.put("v2", vPropMetaData2);

    Map<String, List<PropertyMetaData>> edgeMetaData = new HashMap<>();

    List<PropertyMetaData> ePropMetaData1 = Arrays.asList(
      new PropertyMetaData(
        "eKey1", MetaData.getTypeString(propValues.get(9)), null), // Map
      new PropertyMetaData(
        "eKey2", MetaData.getTypeString(propValues.get(7)), null) // BigDecimal
    );
    edgeMetaData.put("e1", ePropMetaData1);

    List<PropertyMetaData> ePropMetaData2 = Arrays.asList(
      new PropertyMetaData(
        "eKey3", MetaData.getTypeString(propValues.get(6)), null), // String
      new PropertyMetaData(
        "eKey4", MetaData.getTypeString(propValues.get(3)), null) // Long
    );

    edgeMetaData.put("e2", ePropMetaData2);

    MetaData metaData = new MetaData(graphMetaData, vertexMetaData, edgeMetaData);

    assertTrue(metaData.getGraphLabels().contains("g1"));
    assertTrue(metaData.getGraphLabels().contains("g2"));

    assertTrue(metaData.getVertexLabels().contains("v1"));
    assertTrue(metaData.getVertexLabels().contains("v2"));

    assertTrue(metaData.getEdgeLabels().contains("e1"));
    assertTrue(metaData.getEdgeLabels().contains("e2"));

    assertEquals(gPropMetaData1, metaData.getGraphPropertyMetaData("g1"));
    assertEquals(gPropMetaData2, metaData.getGraphPropertyMetaData("g2"));

    assertEquals(vPropMetaData1, metaData.getVertexPropertyMetaData("v1"));
    assertEquals(vPropMetaData2, metaData.getVertexPropertyMetaData("v2"));

    assertEquals(ePropMetaData1, metaData.getEdgePropertyMetaData("e1"));
    assertEquals(ePropMetaData2, metaData.getEdgePropertyMetaData("e2"));
  }

  /**
   * Test the conversion of property value types to strings and to classes.
   */
  @Test
  public void testClassFromStringType() {

    for (int i = 0; i < propValues.size(); i++) {
      assertEquals(
        propTypes.get(i),
        MetaData.getClassFromTypeString(
          MetaData.getTypeString(propValues.get(i)))
      );
    }
  }
}
