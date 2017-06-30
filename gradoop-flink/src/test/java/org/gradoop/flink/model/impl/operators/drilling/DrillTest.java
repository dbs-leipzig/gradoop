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
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.DrillDownMultiplyByOneK;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.DrillDownMultiplyByTen;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.RollUpDivideByOneK;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.RollUpHoursToDays;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.RollUpMillisToSeconds;
import org.gradoop.flink.model.impl.operators.drilling.drillfunctions.RollUpSecondsToMinutes;
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
        .setFunction(new RollUpDivideByOneK())
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
        .setFunction(new RollUpDivideByOneK())
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
        .setFunction(new RollUpDivideByOneK())
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
        .setFunction(new RollUpDivideByOneK())
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new RollUpDivideByOneK())
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
        .setFunction(new RollUpDivideByOneK())
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_in_K")
        .setNewPropertyKey("memberCount_in_M")
        .setFunction(new RollUpDivideByOneK())
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
        .setFunction(new RollUpMillisToSeconds())
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setFunction(new RollUpSecondsToMinutes())
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setFunction(new RollUpSecondsToMinutes())
        .drillVertex(true)
        .buildRollUp())
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("birth")
        .setNewPropertyKey("birthDays")
        .setFunction(new RollUpHoursToDays())
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
        .setFunction(new RollUpMillisToSeconds())
        .drillEdge(true)
        .buildRollUp());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  //----------------------------------------------------------------------------
  // Tests for drill down
  //----------------------------------------------------------------------------


  @Test(expected = NullPointerException.class)
  public void testVertexDrillDownNoProperty() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input.drillDown(new Drill.DrillBuilder()
      .drillVertex(true)
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
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillVertex(true)
        .buildDrillDown());

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
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_times_K")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillVertex(true)
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
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillVertex(true)
        .buildDrillDown())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillVertex(true)
        .buildDrillDown());

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
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_times_K")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillVertex(true)
        .buildDrillDown())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_times_K")
        .setNewPropertyKey("memberCount_times_M")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillVertex(true)
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
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("birthMillis")
        .setNewPropertyKey("birthLowerMillis")
        .setFunction(new DrillDownMultiplyByTen())
        .drillVertex(true)
        .buildDrillDown())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("birthLowerMillis")
        .setFunction(new DrillDownMultiplyByTen())
        .drillVertex(true)
        .buildDrillDown())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("birthLowerMillis")
        .setFunction(new DrillDownMultiplyByTen())
        .drillVertex(true)
        .buildDrillDown())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("birthLowerMillis")
        .setNewPropertyKey("birthTenNanos")
        .setFunction(new DrillDownMultiplyByTen())
        .drillVertex(true)
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
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("until")
        .setFunction(new DrillDownMultiplyByOneK())
        .drillEdge(true)
        .buildDrillDown());

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
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setFunction(new RollUpDivideByOneK())
        .drillVertex(true)
        .buildRollUp())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .drillVertex(true)
        .buildDrillDown());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("input")));
  }

  @Test
  public void testVertexDrillDownAfterRollUpNewPropertyKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getDrillInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = input
      .rollUp(new Drill.DrillBuilder()
        .setPropertyKey("memberCount")
        .setNewPropertyKey("memberCount_in_K")
        .setFunction(new RollUpDivideByOneK())
        .drillVertex(true)
        .buildRollUp())
      .drillDown(new Drill.DrillBuilder()
        .setPropertyKey("memberCount_in_K")
        .setNewPropertyKey("memberCount")
        .drillVertex(true)
        .buildDrillDown());

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("input")));
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
