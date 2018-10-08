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
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Abstract parent class of csv test classes with common functions for source and sink tests
 */
abstract class CSVTestBase extends GradoopFlinkTestBase {

  /**
   * Global map to define properties of vertices and edges
   */
  private static Map<String, Object> PROPERTY_MAP = getPropertyMap();

  /**
   * Function to initiate the static property map
   *
   * @return a map containing the properties
   */
  private static Map<String, Object> getPropertyMap() {
    LocalDate localDate = LocalDate.of(2018, 6, 1);
    LocalTime localTime = LocalTime.of(18, 6, 1);

    PropertyValue stringValue1 = PropertyValue.create("myString1");
    PropertyValue stringValue2 = PropertyValue.create("myString2");
    ArrayList<PropertyValue> stringList = new ArrayList<>();
    stringList.add(stringValue1);
    stringList.add(stringValue2);

    ArrayList<PropertyValue> intList = new ArrayList<>();
    intList.add(PropertyValue.create(1234));
    intList.add(PropertyValue.create(5678));

    Map<PropertyValue, PropertyValue> objectMap = new HashMap<>();
    objectMap.put(stringValue1, PropertyValue.create(12.345));
    objectMap.put(stringValue2, PropertyValue.create(67.89));

    Set<PropertyValue> stringSet = new HashSet<>();
    stringSet.add(stringValue1);
    stringSet.add(stringValue2);

    Set<PropertyValue> intSet = new HashSet<>();
    intSet.add(PropertyValue.create(1234));
    intSet.add(PropertyValue.create(5678));

    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(GradoopTestUtils.KEY_0, GradoopTestUtils.BOOL_VAL_1);
    propertyMap.put(GradoopTestUtils.KEY_1, GradoopTestUtils.INT_VAL_2);
    propertyMap.put(GradoopTestUtils.KEY_2, GradoopTestUtils.LONG_VAL_3);
    propertyMap.put(GradoopTestUtils.KEY_3, GradoopTestUtils.FLOAT_VAL_4);
    propertyMap.put(GradoopTestUtils.KEY_4, GradoopTestUtils.DOUBLE_VAL_5);
    propertyMap.put(GradoopTestUtils.KEY_5, GradoopTestUtils.STRING_VAL_6);
    propertyMap.put(GradoopTestUtils.KEY_6, GradoopId.fromString("000000000000000000000001"));
    propertyMap.put(GradoopTestUtils.KEY_7, localDate);
    propertyMap.put(GradoopTestUtils.KEY_8, localTime);
    propertyMap.put(GradoopTestUtils.KEY_9, LocalDateTime.of(localDate, localTime));
    propertyMap.put(GradoopTestUtils.KEY_a, GradoopTestUtils.BIG_DECIMAL_VAL_7);
    propertyMap.put(GradoopTestUtils.KEY_b, objectMap);
    propertyMap.put(GradoopTestUtils.KEY_c, stringList);
    propertyMap.put(GradoopTestUtils.KEY_d, intList);
    propertyMap.put(GradoopTestUtils.KEY_e, GradoopTestUtils.SHORT_VAL_e);
    propertyMap.put(GradoopTestUtils.KEY_f, GradoopTestUtils.NULL_VAL_0);
    propertyMap.put(GradoopTestUtils.KEY_g, stringSet);
    propertyMap.put(GradoopTestUtils.KEY_h, intSet);
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
    GradoopId idForum = GradoopId.get();
    GradoopIdSet heads = GradoopIdSet.fromExisting(idForum);
    Properties properties = Properties.createFromMap(PROPERTY_MAP);

    DataSet<GraphHead> graphHead = getExecutionEnvironment().fromElements(
      new GraphHead(idForum, "Forum", properties)
    );

    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(
      new Vertex(idUser, "User", properties, heads),
      new Vertex(idPost, "Post", properties, heads)
    );

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new Edge(GradoopId.get(), "creatorOf", idUser, idPost, properties, heads)
    );

    return getConfig().getLogicalGraphFactory().fromDataSets(graphHead, vertices, edges);
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
    assertEquals(PROPERTY_MAP.size(), epgmElement.getPropertyCount());
    // assert that there are properties with keys "h", "i" and "j"
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_0));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_1));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_2));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_3));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_4));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_5));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_6));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_7));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_8));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_9));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_a));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_b));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_c));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_d));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_e));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_f));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_g));
    assertTrue(epgmElement.hasProperty(GradoopTestUtils.KEY_h));

    // assert that the properties have valid data types
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_0).isBoolean());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_1).isInt());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_2).isLong());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_3).isFloat());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_4).isDouble());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_5).isString());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_6).isGradoopId());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_7).isDate());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_8).isTime());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_9).isDateTime());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_a).isBigDecimal());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_b).isMap());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_c).isList());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_d).isList());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_e).isShort());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_f).isNull());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_g).isSet());
    assertTrue(epgmElement.getPropertyValue(GradoopTestUtils.KEY_h).isSet());

    // assert that the properties have valid values
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_0).getBoolean(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_0));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_1).getInt(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_1));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_2).getLong(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_2));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_3).getFloat(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_3));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_4).getDouble(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_4));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_5).getString(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_5));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_6).getGradoopId(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_6));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_7).getDate(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_7));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_8).getTime(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_8));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_9).getDateTime(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_9));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_a).getBigDecimal(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_a));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_b).getMap(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_b));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_c).getList(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_c));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_d).getList(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_d));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_e).getShort(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_e));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_f).getObject(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_f));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_g).getSet(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_g));
    assertEquals(epgmElement.getPropertyValue(GradoopTestUtils.KEY_h).getSet(),
      PROPERTY_MAP.get(GradoopTestUtils.KEY_h));
  }

  /**
   * Check that a line of the created csv metadata file has all expected data type definitions
   *
   * @param line the line of a csv metadata file as string
   */
  protected void checkMetadataCsvLine(String line) {
    assertTrue(line.contains(GradoopTestUtils.KEY_0 + ":boolean"));
    assertTrue(line.contains(GradoopTestUtils.KEY_1 + ":int"));
    assertTrue(line.contains(GradoopTestUtils.KEY_2 + ":long"));
    assertTrue(line.contains(GradoopTestUtils.KEY_3 + ":float"));
    assertTrue(line.contains(GradoopTestUtils.KEY_4 + ":double"));
    assertTrue(line.contains(GradoopTestUtils.KEY_5 + ":string"));
    assertTrue(line.contains(GradoopTestUtils.KEY_6 + ":gradoopid"));
    assertTrue(line.contains(GradoopTestUtils.KEY_7 + ":localdate"));
    assertTrue(line.contains(GradoopTestUtils.KEY_8 + ":localtime"));
    assertTrue(line.contains(GradoopTestUtils.KEY_9 + ":localdatetime"));
    assertTrue(line.contains(GradoopTestUtils.KEY_a + ":bigdecimal"));
    assertTrue(line.contains(GradoopTestUtils.KEY_b + ":map:string:double"));
    assertTrue(line.contains(GradoopTestUtils.KEY_c + ":list:string"));
    assertTrue(line.contains(GradoopTestUtils.KEY_d + ":list:int"));
    assertTrue(line.contains(GradoopTestUtils.KEY_e + ":short"));
    assertTrue(line.contains(GradoopTestUtils.KEY_f + ":null"));
    assertTrue(line.contains(GradoopTestUtils.KEY_g + ":set:string"));
    assertTrue(line.contains(GradoopTestUtils.KEY_h + ":set:int"));
  }
}
