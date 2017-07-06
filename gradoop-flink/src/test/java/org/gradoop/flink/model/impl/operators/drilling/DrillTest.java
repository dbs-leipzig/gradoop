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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.RollUpDivideBy;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;


public class DrillTest extends GradoopFlinkTestBase {

  //----------------------------------------------------------------------------
  // Tests for roll up
  //----------------------------------------------------------------------------


  @Test(expected = NullPointerException.class)
  public void testVertexRollUpNoProperty() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input.rollUp(new Drill.DrillBuilder()
      .drillVertex(true)
      .buildRollUp());
  }

  @Test(expected = NullPointerException.class)
  public void testVertexRollUpFunctionOnly() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input
      .rollUp(new Drill.DrillBuilder()
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
        .buildRollUp());

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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_in_K")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
        .buildRollUp());

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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_in_K")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_in_K")
        .setNewPropertyKey("memberCount_in_M")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birthMillis")
        .setNewPropertyKey("birth")
        .setFunction(new RollUpDivideBy(1000L))
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setFunction(new RollUpDivideBy(60L))
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setFunction(new RollUpDivideBy(60L))
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setNewPropertyKey("birthDays")
        .setFunction(new RollUpDivideBy(24L))
        .drillVertex(true)
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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("until")
        .setFunction(new RollUpDivideBy(1000L))
        .drillEdge(true)
        .buildRollUp());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  private String getDrillInput() {
    return "input[" +
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
