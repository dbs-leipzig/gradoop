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
package org.gradoop.flink.model.impl.operators.grouping;

import com.google.common.collect.ImmutableList;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * This class tests the correct instantiation of {@link Grouping} operators using
 * its {@link Grouping.GroupingBuilder}.
 */
public class GroupingBuilderTest {

  /**
   * Tests {@link Grouping.GroupingBuilder#build()}, expects a correct exception when no
   * {@link GroupingStrategy} was set.
   */
  @Test(expected = IllegalStateException.class)
  public void testNoStrategySetError() {

    new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .build();
  }

  /**
   * Tests {@link Grouping.GroupingBuilder#build()} with strategy set to
   * {@link GroupingStrategy#GROUP_REDUCE}.
   */
  @Test
  public void testGroupReduceStrategy() {
    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .useVertexLabel(true)
      .build();
    assertFalse(grouping instanceof GroupingGroupCombine);
    assertTrue(grouping instanceof GroupingGroupReduce);
  }

  /**
   * Tests {@link Grouping.GroupingBuilder#build()} with strategy set to
   * {@link GroupingStrategy#GROUP_COMBINE}.
   */
  @Test
  public void testGroupCombineStrategy() {
    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_COMBINE)
      .useVertexLabel(true)
      .build();
    assertFalse(grouping instanceof GroupingGroupReduce);
    assertTrue(grouping instanceof GroupingGroupCombine);
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when a strategy is set and
   * when only grouping by vertex labels.
   */
  @Test
  public void testGroupByVertexLabels() {
    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .build();

    assertTrue(grouping instanceof Grouping);
    assertTrue(((Grouping) grouping).useVertexLabels());
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when a strategy is set and
   * when only grouping by vertex labels.
   */
  @Test
  public void testGroupByEdgeLabels() {

    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping = new Grouping.GroupingBuilder()
      .useEdgeLabel(true)
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .build();

    assertTrue(grouping instanceof Grouping);
    assertTrue(((Grouping) grouping).useEdgeLabels());
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when only grouping by
   * one vertex property.
   */
  @Test
  public void testGroupByVertexProperty() {

    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .addVertexGroupingKey("")
      .build();

    assertTrue(grouping instanceof Grouping);
    assertTrue(((Grouping) grouping).useVertexProperties());
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when only grouping by
   * one edge property.
   */
  @Test
  public void testGroupByEdgeProperty() {

    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .addEdgeGroupingKey("")
      .build();

    assertTrue(grouping instanceof Grouping);
    assertTrue(((Grouping) grouping).useEdgeProperties());
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when using specific
   * vertex label grouping.
   */
  @Test
  public void testVertexLabelSpecificGroup() {

    String testGroupLabel = "testGroup";
    ImmutableList<String> testGroupingKeys = ImmutableList.of("a", "b");

    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping =
      new Grouping.GroupingBuilder()
        .setStrategy(GroupingStrategy.GROUP_REDUCE)
        .addVertexLabelGroup(testGroupLabel, testGroupingKeys)
        .build();

    assertTrue(grouping instanceof Grouping);
    List<LabelGroup> vertexLabelGroups =
      ((Grouping<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>) grouping)
        .getVertexLabelGroups();

    // first element of vertexLabelGroups is always the defaultVertexLabelGroup, so the label
    // specific group should be at index 1.
    LabelGroup group = vertexLabelGroups.get(1);

    assertEquals(group.getGroupLabel(), testGroupLabel);
    assertEquals(testGroupingKeys, group.getPropertyKeys());
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when using specific
   * edge label grouping.
   */
  @Test
  public void testEdgeLabelSpecificGroup() {

    String testGroupLabel = "testGroup";
    ImmutableList<String> testGroupingKeys = ImmutableList.of("a", "b");

    UnaryBaseGraphToBaseGraphOperator<LogicalGraph> grouping =
      new Grouping.GroupingBuilder()
        .setStrategy(GroupingStrategy.GROUP_REDUCE)
        .addEdgeLabelGroup(testGroupLabel, testGroupingKeys)
        .build();

    List<LabelGroup> edgeLabelGroups =
      ((Grouping<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>) grouping)
        .getEdgeLabelGroups();

    // first element of edgeLabelGroups is always the defaultEdgeLabelGroup, so the label
    // specific group should be at index 1.
    LabelGroup group = edgeLabelGroups.get(1);

    assertEquals(group.getGroupLabel(), testGroupLabel);
    assertEquals(testGroupingKeys, group.getPropertyKeys());
  }

}
