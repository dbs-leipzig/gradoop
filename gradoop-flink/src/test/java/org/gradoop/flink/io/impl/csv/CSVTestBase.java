/**
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
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Abstract parent class of csv test classes with common functions for source and sink tests
 */
abstract class CSVTestBase extends GradoopFlinkTestBase {

  private static String PROP_A = "a";
  private static String PROP_B = "b";
  private static String PROP_C = "c";
  private static String PROP_D = "d";
  private static String PROP_E = "e";
  private static String PROP_F = "f";
  private static String PROP_G = "g";
  private static String PROP_H = "h";
  private static String PROP_I = "i";
  private static String PROP_J = "j";
  private static String PROP_K = "k";
  private static String PROP_L = "l";
  private static String PROP_M = "m";

  private static Map<String, Object> PROP_MAP = getPropertyMap();

  private static Map<String, Object> getPropertyMap() {
    LocalDate localDate = LocalDate.of(2018, 6, 1);
    LocalTime localTime = LocalTime.of(18, 6, 1);

    PropertyValue stringValue1 = PropertyValue.create("myString1");
    PropertyValue stringValue2 = PropertyValue.create("myString2");
    ArrayList<PropertyValue> stringList = new ArrayList<>();
    stringList.add(stringValue1);
    stringList.add(stringValue2);

    Map<PropertyValue, PropertyValue> objectMap = new HashMap<>();
    objectMap.put(stringValue1, PropertyValue.create("myValue1"));
    objectMap.put(stringValue2, PropertyValue.create("myValue2"));

    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(PROP_A, true);
    propertyMap.put(PROP_B, 1234);
    propertyMap.put(PROP_C, 44L);
    propertyMap.put(PROP_D, (float) 3.14);
    propertyMap.put(PROP_E, 3.14);
    propertyMap.put(PROP_F, "test");
    propertyMap.put(PROP_G, GradoopId.fromString("000000000000000000000001"));
    propertyMap.put(PROP_H, localDate);
    propertyMap.put(PROP_I, localTime);
    propertyMap.put(PROP_J, LocalDateTime.of(localDate, localTime));
    propertyMap.put(PROP_K, new BigDecimal("0.33"));
    propertyMap.put(PROP_L, objectMap);
    propertyMap.put(PROP_M, stringList);
    return Collections.unmodifiableMap(propertyMap);
  }

  /**
   * Get a logical graph with the schema and all properties of "input_extended_properties" csv
   * graph from resources directory. A GDL file can not be used while types like
   * LocalDate etc. are not supported.
   *
   * @return the logical graph representing the expected graph
   */
  protected LogicalGraph getExtendedLogicalGraph() {
    GradoopId idUser = GradoopId.get();
    GradoopId idPost = GradoopId.get();
    GradoopIdSet heads = GradoopIdSet.fromExisting();
    Properties properties = Properties.createFromMap(PROP_MAP);

    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(
      new Vertex(idUser, "User", properties, heads),
      new Vertex(idPost, "Post", properties, heads)
    );

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new Edge(GradoopId.get(), "creatorOf", idUser, idPost, properties, heads)
    );

    return getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
  }

  /**
   * Check all time properties of an epgm element
   *
   * @param epgmElement the element to check
   */
  protected void checkProperties(EPGMElement epgmElement) {
    // assert that the element has properties
    assertNotNull(epgmElement.getProperties());
    // assert that there are 3 properties
    assertEquals( PROP_MAP.size(), epgmElement.getPropertyCount());
    // assert that there are properties with keys "h", "i" and "j"
    assertTrue(epgmElement.hasProperty(PROP_A));
    assertTrue(epgmElement.hasProperty(PROP_B));
    assertTrue(epgmElement.hasProperty(PROP_C));
    assertTrue(epgmElement.hasProperty(PROP_D));
    assertTrue(epgmElement.hasProperty(PROP_E));
    assertTrue(epgmElement.hasProperty(PROP_F));
    assertTrue(epgmElement.hasProperty(PROP_G));
    assertTrue(epgmElement.hasProperty(PROP_H));
    assertTrue(epgmElement.hasProperty(PROP_I));
    assertTrue(epgmElement.hasProperty(PROP_J));
    assertTrue(epgmElement.hasProperty(PROP_K));
    assertTrue(epgmElement.hasProperty(PROP_L));
    assertTrue(epgmElement.hasProperty(PROP_M));

    // assert that the properties have valid data types
    assertTrue(epgmElement.getPropertyValue(PROP_A).isBoolean());
    assertTrue(epgmElement.getPropertyValue(PROP_B).isInt());
    assertTrue(epgmElement.getPropertyValue(PROP_C).isLong());
    assertTrue(epgmElement.getPropertyValue(PROP_D).isFloat());
    assertTrue(epgmElement.getPropertyValue(PROP_E).isDouble());
    assertTrue(epgmElement.getPropertyValue(PROP_F).isString());
    assertTrue(epgmElement.getPropertyValue(PROP_G).isGradoopId());
    assertTrue(epgmElement.getPropertyValue(PROP_H).isDate());
    assertTrue(epgmElement.getPropertyValue(PROP_I).isTime());
    assertTrue(epgmElement.getPropertyValue(PROP_J).isDateTime());
    assertTrue(epgmElement.getPropertyValue(PROP_K).isBigDecimal());
    assertTrue(epgmElement.getPropertyValue(PROP_L).isMap());
    assertTrue(epgmElement.getPropertyValue(PROP_M).isList());

    // assert that the properties have valid values
    assertEquals(epgmElement.getPropertyValue(PROP_A).getBoolean(), PROP_MAP.get(PROP_A));
    assertEquals(epgmElement.getPropertyValue(PROP_B).getInt(), PROP_MAP.get(PROP_B));
    assertEquals(epgmElement.getPropertyValue(PROP_C).getLong(), PROP_MAP.get(PROP_C));
    assertEquals(epgmElement.getPropertyValue(PROP_D).getFloat(), PROP_MAP.get(PROP_D));
    assertEquals(epgmElement.getPropertyValue(PROP_E).getDouble(), PROP_MAP.get(PROP_E));
    assertEquals(epgmElement.getPropertyValue(PROP_F).getString(), PROP_MAP.get(PROP_F));
    assertEquals(epgmElement.getPropertyValue(PROP_G).getGradoopId(), PROP_MAP.get(PROP_G));
    assertEquals(epgmElement.getPropertyValue(PROP_H).getDate(), PROP_MAP.get(PROP_H));
    assertEquals(epgmElement.getPropertyValue(PROP_I).getTime(), PROP_MAP.get(PROP_I));
    assertEquals(epgmElement.getPropertyValue(PROP_J).getDateTime(), PROP_MAP.get(PROP_J));
    assertEquals(epgmElement.getPropertyValue(PROP_K).getBigDecimal(), PROP_MAP.get(PROP_K));
    assertEquals(epgmElement.getPropertyValue(PROP_L).getMap(), PROP_MAP.get(PROP_L));
    assertEquals(epgmElement.getPropertyValue(PROP_M).getList(), PROP_MAP.get(PROP_M));
  }
}
