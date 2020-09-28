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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

public class FilterAndProjectTemporalEdgesTest {
  TemporalEdgeFactory factory = new TemporalEdgeFactory();

  @Test
  public void testMetaDataInitialization() {
    String sourceVariable = "a";
    String edgeVariable = "e";
    String targetVariable = "b";
    FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(
      null, sourceVariable, edgeVariable, targetVariable, new CNF(), new HashSet<>(), false);

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertThat(embeddingMetaData.getEntryColumn(sourceVariable), is(0));
    assertThat(embeddingMetaData.getEntryColumn(edgeVariable), is(1));
    assertThat(embeddingMetaData.getEntryColumn(targetVariable), is(2));
    assertThat(embeddingMetaData.getPropertyKeys(edgeVariable).size(), is(0));
  }

  @Test
  public void testMetaDataInitializationWithLoop() {
    String sourceVariable = "a";
    String edgeVariable = "e";
    String targetVariable = "a";
    FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(
      null, sourceVariable, edgeVariable, targetVariable, new CNF(), new HashSet<>(), false);

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertThat(embeddingMetaData.getEntryColumn(sourceVariable), is(0));
    assertThat(embeddingMetaData.getEntryColumn(edgeVariable), is(1));
    assertThat(embeddingMetaData.getEntryColumn(targetVariable), is(0));
    assertThat(embeddingMetaData.getPropertyKeys(edgeVariable).size(), is(0));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();

    GradoopId edge1Id = GradoopId.get();
    Map<String, Object> edge1Props = new HashMap<>();
    edge1Props.put("foo", 23);
    Long[] edge1TimeData = new Long[] {123L, 1234L, 345L, 456L};

    GradoopId edge2Id = GradoopId.get();
    Map<String, Object> edge2Props = new HashMap<>();
    edge2Props.put("foo", 42);
    Long[] edge2TimeData = new Long[] {123L, 1234L, 345L, 456L};

    GradoopId edge3Id = GradoopId.get();
    Map<String, Object> edge3Props = new HashMap<>();
    edge3Props.put("foo", 23);
    Long[] edge3TimeData = new Long[] {12345L, 1234567L, 3452L, 456789L};


    TemporalEdge e1 =
      factory.initEdge(edge1Id, "a", sourceId, targetId, Properties.createFromMap(edge1Props));
    e1.setTransactionTime(new Tuple2<>(edge1TimeData[0], edge1TimeData[1]));
    e1.setValidTime(new Tuple2<>(edge1TimeData[2], edge1TimeData[3]));

    TemporalEdge e2 =
      factory.initEdge(edge2Id, "b", sourceId, targetId, Properties.createFromMap(edge2Props));
    e2.setTransactionTime(new Tuple2<>(edge2TimeData[0], edge2TimeData[1]));
    e2.setValidTime(new Tuple2<>(edge2TimeData[2], edge2TimeData[3]));

    TemporalEdge e3 =
      factory.initEdge(edge3Id, "c", sourceId, targetId, Properties.createFromMap(edge2Props));
    e3.setTransactionTime(new Tuple2<>(edge3TimeData[0], edge3TimeData[1]));
    e3.setValidTime(new Tuple2<>(edge3TimeData[2], edge3TimeData[3]));

    DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2, e3);

    //only matched by e1
    String query = "MATCH (a)-[e]->(b) WHERE e.foo = 23 " +
      "AND a.val_from <= e.val_from";

    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("e"));
    Set<String> projectionKeys = queryHandler.getPredicates().getPropertyKeys("e");
    System.out.println(projectionKeys);

    FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(edges, "a", "e", "b",
      filterPredicate, projectionKeys, false);

    List<Embedding> filteredEdges = node.execute().collect();

    assertThat(filteredEdges.size(), is(1));
    assertThat(filteredEdges.get(0).getId(0).equals(sourceId), is(true));
    assertThat(filteredEdges.get(0).getId(1).equals(edge1Id), is(true));
    assertThat(filteredEdges.get(0).getId(2).equals(targetId), is(true));


    EmbeddingMetaData metaData = node.getEmbeddingMetaData();
    assertThat(metaData.getEntryColumn("a"), is(0));
    assertThat(metaData.getEntryColumn("e"), is(1));
    assertThat(metaData.getEntryColumn("b"), is(2));
    assertThat(metaData.getPropertyKeys("e").size(), is(3));

    assertEquals(metaData.getPropertyColumn("e", "foo"), 1);
    assertEquals(metaData.getPropertyColumn("e", TimeSelector.TimeField.VAL_FROM.toString()), 2);
    assertEquals(metaData.getPropertyColumn("e", TimeSelector.TimeField.VAL_TO.toString()), 0);
  }
}
