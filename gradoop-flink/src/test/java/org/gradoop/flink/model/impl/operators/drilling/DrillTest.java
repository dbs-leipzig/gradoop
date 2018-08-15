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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.DrillMultiplyBy;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.DrillDivideBy;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;


public class DrillTest extends GradoopFlinkTestBase {

  //----------------------------------------------------------------------------
  // Tests for roll up
  //----------------------------------------------------------------------------


  @SuppressWarnings("unused")
	@Test(expected = NullPointerException.class)
  public void testVertexRollUpNoProperty() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input.callForGraph(new Drill.DrillBuilder()
      .buildRollUp());
  }

  @SuppressWarnings("unused")
	@Test(expected = NullPointerException.class)
  public void testVertexRollUpFunctionOnly() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setVertexDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp());
  }

  @Test
  public void testVertexRollUpPropertyKeyFunction() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    
    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145L,memberCount__1 : 1563145521L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341L,memberCount__1: 451341564L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .rollUpVertex("memberCount", new DrillDivideBy(1000L));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexRollUpPropertyKeyFunctionNewPropertyKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L,memberCount_in_K: 1563145L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L,memberCount_in_K: 451341L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_in_K")
        .setVertexDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp());


    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexRollUpChainedRollUp() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563L,memberCount__1: 1563145521L," +
        "memberCount__2: 1563145L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451L,memberCount__1: 451341564L," +
        "memberCount__2: 451341L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .rollUpVertex("memberCount", new DrillDivideBy(1000L))
      .rollUpVertex("memberCount", new DrillDivideBy(1000L));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexRollUpNewPropertyKeyChainedRollUp() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L,memberCount_in_K: 1563145L," +
        "memberCount_in_M: 1563L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L,memberCount_in_K: 451341L," +
        "memberCount_in_M: 451L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_in_K")
        .setVertexDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_in_K")
        .setNewPropertyKey("memberCount_in_M")
        .setVertexDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexRollUpMixedChainedRollUp() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L,birth : 138888L," +
        "birth__1 : 500000000L,birth__2 : 8333333L,birthDays : 5787L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L,birth : 147222L," +
        "birth__1 : 530000000L,birth__2 : 8833333L,birthDays : 6134L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L,birth : 155555L," +
        "birth__1 : 560000000L,birth__2 : 9333333L,birthDays : 6481L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L,birth : 163888L," +
        "birth__1 : 590000000L,birth__2 : 9833333L,birthDays : 6828L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birthMillis")
        .setNewPropertyKey("birth")
        .setVertexDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setVertexDrillFunction(new DrillDivideBy(60L))
        .buildRollUp())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setVertexDrillFunction(new DrillDivideBy(60L))
        .buildRollUp())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setNewPropertyKey("birthDays")
        .setVertexDrillFunction(new DrillDivideBy(24L))
        .buildRollUp());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }



  @Test
  public void testEdgeRollUpPropertyKeyFunction() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .rollUpEdge("until", new DrillDivideBy(1000L));
    
    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeRollUpUsingDrillBuilder() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000L,until__1 : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input.callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("until")
        .setEdgeDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp());
    
    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  @Test
  public void testGraphHeadRollUpUsingDrillBuilder() throws Exception {
  	FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());
    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString(
    	"expected{title : \"Graph\", globalMemberCount : 42L,globalMemberCount__1: 42000L}[" + 
    	"(v0:Forum {topic : \"rdf\",memberCount : 1563145521L})" + 
    	"(v1:Forum {topic : \"graph\",memberCount: 451341564L})" + 
    	"(v2:User {gender : \"male\",birthMillis : 500000000000L})" + 
    	"(v3:User {gender : \"male\",birthMillis : 530000000000L})" + 
    	"(v4:User {gender : \"male\",birthMillis : 560000000000L})" + 
    	"(v5:User {gender : \"female\",birthMillis : 590000000000L})" + 
    	"(v2)-[:member {until : 1550000000000L}]->(v0)" + 
    	"(v3)-[:member {until : 1550000000000L}]->(v0)" + 
    	"(v3)-[:member {until : 1550000000000L}]->(v1)" + 
    	"(v4)-[:member {until : 1550000000000L}]->(v1)" + 
    	"(v5)-[:member {until : 1550000000000L}]->(v1)" + 
    	"(v2)-[:knows {since : 1350000000000L}]->(v3)" + 
    	"(v3)-[:knows {since : 1350000000000L}]->(v2)" + 
    	"(v3)-[:knows {since : 1350000000000L}]->(v4)" + 
    	"(v3)-[:knows {since : 1350000000000L}]->(v5)" + 
    	"(v5)-[:knows {since : 1350000000000L}]->(v4)" + 
    	"]");
    
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph result = input.callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("globalMemberCount")
        .setGraphheadDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp());
    
    collectAndAssertTrue(result.equalsByData(expected));
  }
  
  //----------------------------------------------------------------------------
  // Tests for drill down
  //----------------------------------------------------------------------------


  @SuppressWarnings("unused")
	@Test(expected = NullPointerException.class)
  public void testVertexDrillDownNoProperty() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input.callForGraph(new Drill.DrillBuilder()
      .buildDrillDown());
  }

  @Test
  public void testVertexDrillDownPropertyKeyFunction() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521000L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .drillDownVertex("memberCount", new DrillMultiplyBy(1000L));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexDrillDownPropertyKeyFunctionNewPropertyKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount_times_K: 1563145521000L})" +
      "(v01:Forum {topic : \"graph\",memberCount_times_K: 451341564000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_times_K")
        .setVertexDrillFunction(new DrillMultiplyBy(1000L))
        .buildDrillDown());


    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexDrillDownChainedDrillDown() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521000000L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564000000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .drillDownVertex("memberCount", new DrillMultiplyBy(1000L))
      .drillDownVertex("memberCount", new DrillMultiplyBy(1000L));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexDrillDownNewPropertyKeyChainedDrillDown() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount_times_M: 1563145521000000L})" +
      "(v01:Forum {topic : \"graph\",memberCount_times_M: 451341564000000L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_times_K")
        .setVertexDrillFunction(new DrillMultiplyBy(1000L))
        .buildDrillDown())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_times_K")
        .setNewPropertyKey("memberCount_times_M")
        .setVertexDrillFunction(new DrillMultiplyBy(1000L))
        .buildDrillDown());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexDrillDownMixedChainedDrillDown() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L})" +
      "(v02:User {gender : \"male\",birthTenNanos : 5000000000000000L})" +
      "(v03:User {gender : \"male\",birthTenNanos : 5300000000000000L})" +
      "(v04:User {gender : \"male\",birthTenNanos : 5600000000000000L})" +
      "(v05:User {gender : \"female\",birthTenNanos : 5900000000000000L})" +
      "(v02)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birthMillis")
        .setNewPropertyKey("birthLowerMillis")
        .setVertexDrillFunction(new DrillMultiplyBy(10L))
        .buildDrillDown())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birthLowerMillis")
        .setVertexDrillFunction(new DrillMultiplyBy(10L))
        .buildDrillDown())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birthLowerMillis")
        .setVertexDrillFunction(new DrillMultiplyBy(10L))
        .buildDrillDown())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("birthLowerMillis")
        .setNewPropertyKey("birthTenNanos")
        .setVertexDrillFunction(new DrillMultiplyBy(10L))
        .buildDrillDown());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }



  @Test
  public void testEdgeDrillDownPropertyKeyFunction() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\",memberCount : 1563145521L})" +
      "(v01:Forum {topic : \"graph\",memberCount: 451341564L})" +
      "(v02:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v03:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v04:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v05:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v02)-[:member {until : 1550000000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000000L}]->(v00)" +
      "(v03)-[:member {until : 1550000000000000L}]->(v01)" +
      "(v04)-[:member {until : 1550000000000000L}]->(v01)" +
      "(v05)-[:member {until : 1550000000000000L}]->(v01)" +
      "(v02)-[:knows {since : 1350000000000L}]->(v03)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v02)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v04)" +
      "(v03)-[:knows {since : 1350000000000L}]->(v05)" +
      "(v05)-[:knows {since : 1350000000000L}]->(v04)" +
      "]");

    LogicalGraph output = input
      .drillDownEdge("until", new DrillMultiplyBy(1000L));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  //----------------------------------------------------------------------------
  // Tests for drill down after roll up
  //----------------------------------------------------------------------------


  @Test
  public void testVertexDrillDownAfterRollUp() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input
      .rollUpVertex("memberCount", new DrillDivideBy(1000L))
      .drillDownVertex("memberCount");

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("input")));
  }

  @Test
  public void testVertexDrillDownAfterRollUpNewPropertyKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_in_K")
        .setVertexDrillFunction(new DrillDivideBy(1000L))
        .buildRollUp())
      .callForGraph(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_in_K")
        .setNewPropertyKey("memberCount")
        .buildDrillDown());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("input")));
  }





  private String getDrillInput() {
    return "input{title : \"Graph\", globalMemberCount : 42000L}[" +
      "(v0:Forum {topic : \"rdf\",memberCount : 1563145521L})" +
      "(v1:Forum {topic : \"graph\",memberCount: 451341564L})" +
      "(v2:User {gender : \"male\",birthMillis : 500000000000L})" +
      "(v3:User {gender : \"male\",birthMillis : 530000000000L})" +
      "(v4:User {gender : \"male\",birthMillis : 560000000000L})" +
      "(v5:User {gender : \"female\",birthMillis : 590000000000L})" +
      "(v2)-[:member {until : 1550000000000L}]->(v0)" +
      "(v3)-[:member {until : 1550000000000L}]->(v0)" +
      "(v3)-[:member {until : 1550000000000L}]->(v1)" +
      "(v4)-[:member {until : 1550000000000L}]->(v1)" +
      "(v5)-[:member {until : 1550000000000L}]->(v1)" +
      "(v2)-[:knows {since : 1350000000000L}]->(v3)" +
      "(v3)-[:knows {since : 1350000000000L}]->(v2)" +
      "(v3)-[:knows {since : 1350000000000L}]->(v4)" +
      "(v3)-[:knows {since : 1350000000000L}]->(v5)" +
      "(v5)-[:knows {since : 1350000000000L}]->(v4)" +
      "]";
  }
}
