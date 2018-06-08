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

import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Abstract parent class of csv test classes with common functions for source and sink tests
 */
abstract class CSVTestBase extends GradoopFlinkTestBase {

  private LocalDate LOCAL_DATE = LocalDate.of(2018, 6, 1);
  private LocalTime LOCAL_TIME = LocalTime.of(18, 6, 1);
  private LocalDateTime LOCAL_DATE_TIME = LocalDateTime.of(LOCAL_DATE, LOCAL_TIME);

  private String PROP_H = "h";
  private String PROP_I = "i";
  private String PROP_J = "j";

  /**
   * Get a logical graph with the schema and all properties of "input_extended_properties" csv
   * graph from resources directory. A GDL file can not be used while types like
   * LocalDate etc. are not supported.
   *
   * @return the logical graph representing the expected graph
   */
  protected LogicalGraph getExtendedLogicalGraph() {
    GradoopId idUser = GradoopId.get();
    Map<String, Object> propertyMapUser = Maps.newHashMap();
    propertyMapUser.put("a", true);
    propertyMapUser.put("b", 1234);
    propertyMapUser.put("c", 44L);
    propertyMapUser.put("d", (float) 3.14);

    GradoopId idPost = GradoopId.get();
    Map<String, Object> propertyMapPost = Maps.newHashMap();
    propertyMapPost.put("e", 3.14);
    propertyMapPost.put("f", "test");
    propertyMapPost.put("g", GradoopId.fromString("000000000000000000000001"));

    GradoopIdSet heads = GradoopIdSet.fromExisting();

    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(
      new Vertex(idUser, "User", Properties.createFromMap(propertyMapUser), heads),
      new Vertex(idPost, "Post", Properties.createFromMap(propertyMapPost), heads)
    );

    Map<String, Object> propertyMapCreatorOf = Maps.newHashMap();
    propertyMapCreatorOf.put(PROP_H, LOCAL_DATE);
    propertyMapCreatorOf.put(PROP_I, LOCAL_TIME);
    propertyMapCreatorOf.put(PROP_J, LOCAL_DATE_TIME);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new Edge(
        GradoopId.get(),
        "creatorOf",
        idUser,
        idPost,
        Properties.createFromMap(propertyMapCreatorOf),
        heads
      )
    );

    return getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
  }

  /**
   * Check all time properties of an epgm element
   *
   * @param epgmElement the element to check
   */
  protected void checkTimeProperties(EPGMElement epgmElement) {
    // assert that the element has properties
    assertNotNull(epgmElement.getProperties());
    // assert that there are 3 properties
    assertEquals( 3, epgmElement.getPropertyCount());
    // assert that there are properties with keys "h", "i" and "j"
    assertTrue(epgmElement.hasProperty(PROP_H));
    assertTrue(epgmElement.hasProperty(PROP_I));
    assertTrue(epgmElement.hasProperty(PROP_J));
    // assert that the properties have valid data types
    assertTrue(epgmElement.getPropertyValue(PROP_H).isDate());
    assertTrue(epgmElement.getPropertyValue(PROP_I).isTime());
    assertTrue(epgmElement.getPropertyValue(PROP_J).isDateTime());
    // assert that the properties have valid values
    assertEquals(epgmElement.getPropertyValue(PROP_H).getDate(), LOCAL_DATE);
    assertEquals(epgmElement.getPropertyValue(PROP_I).getTime(), LOCAL_TIME);
    assertEquals(epgmElement.getPropertyValue(PROP_J).getDateTime(), LOCAL_DATE_TIME);
  }
}
