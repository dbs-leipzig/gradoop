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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.gradoop.common.GradoopTestUtils.call;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.assertEmbedding;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.createEmbedding;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class JoinEmbeddingsNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    leftInputMetaData.setPropertyColumn("v1", "age", 0);
    leftInputMetaData.setPropertyColumn("e1", "since", 1);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e2", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 2);
    rightInputMetaData.setPropertyColumn("v2", "age", 0);
    rightInputMetaData.setPropertyColumn("e2", "since", 1);
    rightInputMetaData.setPropertyColumn("v3", "age", 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftMockNode, rightMockNode,
      singletonList("v2"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    EmbeddingMetaData outputMetaData = node.getEmbeddingMetaData();

    assertThat(outputMetaData.getEntryCount(), is(5));
    assertThat(outputMetaData.getEntryColumn("v1"), is(0));
    assertThat(outputMetaData.getEntryColumn("e1"), is(1));
    assertThat(outputMetaData.getEntryColumn("v2"), is(2));
    assertThat(outputMetaData.getEntryColumn("e2"), is(3));
    assertThat(outputMetaData.getEntryColumn("v3"), is(4));

    assertThat(outputMetaData.getPropertyCount(), is(5));
    assertThat(outputMetaData.getPropertyColumn("v1", "age"), is(0));
    assertThat(outputMetaData.getPropertyColumn("e1", "since"), is(1));
    assertThat(outputMetaData.getPropertyColumn("v2", "age"), is(2));
    assertThat(outputMetaData.getPropertyColumn("e2", "since"), is(3));
    assertThat(outputMetaData.getPropertyColumn("v3", "age"), is(4));
  }

  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void testGetJoinColumns() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e2", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 2);

    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      singletonList("v2"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    assertThat(call(JoinEmbeddingsNode.class, node, "getJoinColumnsLeft"), is(asList(2)));
    assertThat(call(JoinEmbeddingsNode.class, node, "getJoinColumnsRight"), is(asList(0)));
  }

  @Test
  public void testGetDistinctColumnsIsomorphism() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 4);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e3", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v4", EntryType.VERTEX, 2);
    rightInputMetaData.setEntryColumn("e4", EntryType.EDGE, 3);
    rightInputMetaData.setEntryColumn("v5", EntryType.VERTEX, 4);

    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      singletonList("v3"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctVertexColumnsLeft"), is(asList(0, 2, 4)));
    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctVertexColumnsRight"), is(asList(2, 4)));
    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList(1, 3)));
    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctEdgeColumnsRight"), is(asList(1, 3)));
  }

  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void testGetDistinctColumnsHomomorphism() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 4);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e3", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v4", EntryType.VERTEX, 2);
    rightInputMetaData.setEntryColumn("e4", EntryType.EDGE, 3);
    rightInputMetaData.setEntryColumn("v5", EntryType.VERTEX, 4);

    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      singletonList("v3"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctVertexColumnsLeft"), is(asList()));
    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctVertexColumnsRight"), is(asList()));
    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList()));
    assertThat(call(JoinEmbeddingsNode.class, node, "getDistinctEdgeColumnsRight"), is(asList()));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId d = GradoopId.get();
    GradoopId e = GradoopId.get();
    GradoopId f = GradoopId.get();

    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setPropertyColumn("v1", "age", 0);

    Embedding embedding1 = createEmbedding(singletonList(Pair.of(a, singletonList(42))));
    Embedding embedding2 = createEmbedding(singletonList(Pair.of(b, singletonList(23))));

    DataSet<Embedding> leftEmbeddings = getExecutionEnvironment()
      .fromElements(embedding1, embedding2);

    /*
     * ----------------------------------
     * |  v1   | e1    | v2    | v2.age |
     * ----------------------------------
     * | id(a) | id(c) | id(e) |  84    | -> Embedding 3
     * ----------------------------------
     * | id(b) | id(d) | id(f) |  77    | -> Embedding 4
     * ----------------------------------
     */
    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    rightInputMetaData.setPropertyColumn("v2", "age", 0);

    Embedding embedding3 = createEmbedding(asList(
      Pair.of(a, emptyList()),
      Pair.of(c, emptyList()),
      Pair.of(e, singletonList(84))
    ));
    Embedding embedding4 = createEmbedding(asList(
      Pair.of(b, emptyList()),
      Pair.of(d, emptyList()),
      Pair.of(f, singletonList(77))
    ));

    DataSet<Embedding> rightEmbeddings = getExecutionEnvironment()
      .fromElements(embedding3, embedding4);

    MockPlanNode leftChild = new MockPlanNode(leftEmbeddings, leftInputMetaData);
    MockPlanNode rightChild = new MockPlanNode(rightEmbeddings, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      singletonList("v1"), MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    List<Embedding> result = node.execute().collect();
    result.sort(Comparator.comparing(o -> o.getProperty(0))); // sort by property value in column 0

    assertThat(result.size(), is(2));

    assertEmbedding(result.get(0), asList(b, d, f), asList(PropertyValue.create(23), PropertyValue.create(77)));
    assertEmbedding(result.get(1), asList(a, c, e), asList(PropertyValue.create(42), PropertyValue.create(84)));
  }
}
