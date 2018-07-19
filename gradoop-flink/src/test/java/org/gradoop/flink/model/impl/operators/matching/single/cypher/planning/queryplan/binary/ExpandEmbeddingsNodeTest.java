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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;

import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.gradoop.common.GradoopTestUtils.call;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExpandEmbeddingsNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    ExpandEmbeddingsNode node = new ExpandEmbeddingsNode(
      leftMockNode, rightMockNode,
      "v1", "e1", "v2",
      0, 10, ExpandDirection.OUT,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    EmbeddingMetaData outputMetaData = node.getEmbeddingMetaData();

    assertThat(outputMetaData.getEntryCount(), is(3));
    assertThat(outputMetaData.getEntryColumn("v1"), is(0));
    assertThat(outputMetaData.getEntryColumn("e1"), is(1));
    assertThat(outputMetaData.getEntryColumn("v2"), is(2));
    assertThat(outputMetaData.getEntryType("v1"), is(EntryType.VERTEX));
    assertThat(outputMetaData.getEntryType("e1"), is(EntryType.PATH));
    assertThat(outputMetaData.getEntryType("v2"), is(EntryType.VERTEX));
    assertThat(outputMetaData.getDirection("e1"), is(ExpandDirection.OUT));
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
    rightInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    ExpandEmbeddingsNode node = new ExpandEmbeddingsNode(
      leftMockNode, rightMockNode,
      "v3", "e4", "v4",
      0, 10, ExpandDirection.OUT,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    assertThat(call(ExpandEmbeddingsNode.class, node, "getDistinctVertexColumns",
      new Class<?>[] {EmbeddingMetaData.class}, new Object[]{leftInputMetaData}),
      is(asList(0, 2, 4)));
    assertThat(call(ExpandEmbeddingsNode.class, node, "getDistinctEdgeColumns",
      new Class<?>[] {EmbeddingMetaData.class}, new Object[]{leftInputMetaData}),
      is(asList(1, 3)));
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
    rightInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    ExpandEmbeddingsNode node = new ExpandEmbeddingsNode(
      leftMockNode, rightMockNode,
      "v3", "e4", "v4",
      0, 10, ExpandDirection.OUT,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    assertThat(call(ExpandEmbeddingsNode.class, node, "getDistinctVertexColumns",
      new Class<?>[] {EmbeddingMetaData.class}, new Object[]{leftInputMetaData}),
      is(asList()));
    assertThat(call(ExpandEmbeddingsNode.class, node, "getDistinctEdgeColumns",
      new Class<?>[] {EmbeddingMetaData.class}, new Object[]{leftInputMetaData}),
      is(asList()));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId d = GradoopId.get();
    GradoopId e = GradoopId.get();
    GradoopId f = GradoopId.get();
    GradoopId g = GradoopId.get();

    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);

    Embedding embedding1 = createEmbedding(a);
    DataSet<Embedding> leftEmbeddings = getExecutionEnvironment().fromElements(embedding1);

    /*
     * -------------------------
     * |  v1   | e1    | v2    |
     * -------------------------
     * | id(a) | id(b) | id(c) | -> Embedding 2
     * -------------------------
     * | id(c) | id(d) | id(e) | -> Embedding 3
     * -------------------------
     * | id(e) | id(f) | id(g) | -> Embedding 4
     * -------------------------
     */
    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);

    Embedding embedding2 = createEmbedding(a, b, c);
    Embedding embedding3 = createEmbedding(c, d, e);
    Embedding embedding4 = createEmbedding(e, f, g);
    DataSet<Embedding> rightEmbeddings = getExecutionEnvironment()
      .fromElements(embedding2, embedding3, embedding4);

    MockPlanNode leftChild = new MockPlanNode(leftEmbeddings, leftInputMetaData);
    MockPlanNode rightChild = new MockPlanNode(rightEmbeddings, rightInputMetaData);

    ExpandEmbeddingsNode node = new ExpandEmbeddingsNode(leftChild, rightChild,
      "v1", "e1", "v2",
      3, 3, ExpandDirection.OUT,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    /*
     * ---------------------------------------------------
     * |  v1   | e1                              | v2    |
     * ---------------------------------------------------
     * | id(a) | [id(b),id(c),id(d),id(e),id(f)] | id(g) | -> Result
     * ---------------------------------------------------
     */
    List<Embedding> result = node.execute().collect();
    assertThat(result.size(), is(1));
    Embedding embedding = result.get(0);
    assertThat(embedding.getId(0), is(a));
    assertThat(embedding.getIdList(1).size(), is(5));
    assertThat(embedding.getIdList(1).get(0), is(b));
    assertThat(embedding.getIdList(1).get(1), is(c));
    assertThat(embedding.getIdList(1).get(2), is(d));
    assertThat(embedding.getIdList(1).get(3), is(e));
    assertThat(embedding.getIdList(1).get(4), is(f));
    assertThat(embedding.getId(2), is(g));
  }
}
