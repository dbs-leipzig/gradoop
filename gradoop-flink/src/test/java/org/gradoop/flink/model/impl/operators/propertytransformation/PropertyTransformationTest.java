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
package org.gradoop.flink.model.impl.operators.propertytransformation;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;


public class PropertyTransformationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145L,memberCount__1 : 1563145521L,generalAttribute : 42000L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341L,memberCount__1: 451341564L,generalAttribute : 42000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
      "(v02)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>(null, "memberCount", new DivideBy(1000L), null, null, null));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexTransformationNewPropKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",dividedMemCount : 1563145L,memberCount : 1563145521L,generalAttribute : 42000L})" +
      "(v01:Forum {topic : \"graph\",dividedMemCount: 451341L,memberCount: 451341564L,generalAttribute : 42000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
      "(v02)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>(null, "memberCount", new DivideBy(1000L), null, null, "dividedMemCount"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexTransformationLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42L,generalAttribute__1 : 42000L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42L,generalAttribute__1 : 42000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
      "(v02)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>("Forum", "generalAttribute", new DivideBy(1000L), null, null, null));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
  
  @Test
  public void testEdgeTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42000L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
      "(v02)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>(null, "generalAttribute", null, new DivideBy(1000L), null, null));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeTransformationNewPropKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
        "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42000L})" +
        "(v01:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42000L})" +
        "(v02:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
        "(v03:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
        "(v04:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
        "(v05:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
        "(v02)-[:member {until : 1550000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v00)" +
        "(v03)-[:member {until : 1550000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v00)" +
        "(v03)-[:member {until : 1550000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v01)" +
        "(v04)-[:member {until : 1550000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v01)" +
        "(v05)-[:member {until : 1550000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v01)" +
        "(v02)-[:knows {since : 1350000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v03)" +
        "(v03)-[:knows {since : 1350000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v02)" +
        "(v03)-[:knows {since : 1350000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v04)" +
        "(v03)-[:knows {since : 1350000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v05)" +
        "(v05)-[:knows {since : 1350000000000L,dividedGA : 42L,generalAttribute : 42000L}]->(v04)" +
        "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>(null, "generalAttribute", null, new DivideBy(1000L), null, "dividedGA"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeTransformationLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
        "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42000L})" +
        "(v01:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42000L})" +
        "(v02:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
        "(v03:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
        "(v04:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
        "(v05:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
        "(v02)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v00)" +
        "(v03)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v00)" +
        "(v03)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v01)" +
        "(v04)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v01)" +
        "(v05)-[:member {until : 1550000000000L,generalAttribute : 42L,generalAttribute__1 : 42000L}]->(v01)" +
        "(v02)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v03)" +
        "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v02)" +
        "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
        "(v03)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v05)" +
        "(v05)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v04)" +
        "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>("member", "generalAttribute", null, new DivideBy(1000L), null, null));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
  
  @Test
  public void testGHTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected:firstLabel{title : \"Graph\",globalMemberCount : 42000L,generalAttribute : 42L,generalAttribute__1 : 42000L}[" +
        "(v0:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42000L})" +
        "(v1:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42000L})" +
        "(v2:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
        "(v3:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
        "(v4:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
        "(v5:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
        "(v2)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v0)" +
        "(v3)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v0)" +
        "(v3)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
        "(v4)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
        "(v5)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
        "(v2)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v3)" +
        "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v2)" +
        "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v4)" +
        "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v5)" +
        "(v5)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v4)" +
        "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>(null, "generalAttribute", null, null, new DivideBy(1000L), null));

    output.print();
    loader.getLogicalGraphByVariable("expected").print();
    
    collectAndAssertTrue(
      output.equalsByData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGHTransformationNewPropKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected:firstLabel{title : \"Graph\",globalMemberCount : 42000L,divided : 42L,generalAttribute : 42000L}[" +
        "(v0:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42000L})" +
        "(v1:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42000L})" +
        "(v2:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
        "(v3:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
        "(v4:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
        "(v5:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
        "(v2)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v0)" +
        "(v3)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v0)" +
        "(v3)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
        "(v4)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
        "(v5)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
        "(v2)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v3)" +
        "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v2)" +
        "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v4)" +
        "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v5)" +
        "(v5)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v4)" +
        "]");

    LogicalGraph output = input
      .callForGraph(new PropertyTransformation<>(null, "generalAttribute", null, null, new DivideBy(1000L), "divided"));

    output.print();
    loader.getLogicalGraphByVariable("expected").print();
    
    collectAndAssertTrue(
      output.equalsByData(loader.getLogicalGraphByVariable("expected")));
  }
  
  private String getInput() {
    return "input:firstLabel{title : \"Graph\",globalMemberCount : 42000L,generalAttribute : 42000L}[" +
      "(v0:Forum {topic : \"rdf\",memberCount : 1563145521L,generalAttribute : 42000L})" +
      "(v1:Forum {topic : \"graph\",memberCount: 451341564L,generalAttribute : 42000L})" +
      "(v2:User {gender : \"male\",birthMillis : 500000000000L,generalAttribute : 42000L})" +
      "(v3:User {gender : \"male\",birthMillis : 530000000000L,generalAttribute : 42000L})" +
      "(v4:User {gender : \"male\",birthMillis : 560000000000L,generalAttribute : 42000L})" +
      "(v5:User {gender : \"female\",birthMillis : 590000000000L,generalAttribute : 42000L})" +
      "(v2)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v0)" +
      "(v3)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v0)" +
      "(v3)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
      "(v4)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
      "(v5)-[:member {until : 1550000000000L,generalAttribute : 42000L}]->(v1)" +
      "(v2)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v3)" +
      "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v2)" +
      "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v4)" +
      "(v3)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v5)" +
      "(v5)-[:knows {since : 1350000000000L,generalAttribute : 42000L}]->(v4)" +
      "]";
  }
}
