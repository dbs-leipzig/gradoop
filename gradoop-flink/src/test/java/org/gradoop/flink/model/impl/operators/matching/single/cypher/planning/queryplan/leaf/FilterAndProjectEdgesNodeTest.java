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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FilterAndProjectEdgesNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    String sourceVariable = "a";
    String edgeVariable   = "e";
    String targetVariable = "b";
    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(
      null, sourceVariable, edgeVariable, targetVariable, new CNF(), new HashSet<>(), false);

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertThat(embeddingMetaData.getEntryColumn(sourceVariable), is(0));
    assertThat(embeddingMetaData.getEntryColumn(edgeVariable), is(1));
    assertThat(embeddingMetaData.getEntryColumn(targetVariable), is(2));
    assertThat(embeddingMetaData.getPropertyKeys(edgeVariable).size(), is(0));
  }

  @Test
  public void testMetaDataInitializationWithLoop() throws Exception {
    String sourceVariable = "a";
    String edgeVariable   = "e";
    String targetVariable = "a";
    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(
      null, sourceVariable, edgeVariable, targetVariable, new CNF(), new HashSet<>(),false);

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

    GradoopId edge2Id = GradoopId.get();
    Map<String, Object> edge2Props = new HashMap<>();
    edge2Props.put("foo", 42);

    Edge e1 = new Edge(edge1Id, "a", sourceId, targetId, Properties.createFromMap(edge1Props), new GradoopIdSet());
    Edge e2 = new Edge(edge2Id, "b", sourceId, targetId, Properties.createFromMap(edge2Props), new GradoopIdSet());

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(e1, e2);

    String query = "MATCH (a)-[e]->(b) WHERE e.foo = 23";
    QueryHandler queryHandler = new QueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("e"));
    Set<String> projectionKeys = queryHandler.getPredicates().getPropertyKeys("e");

    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(
      edges, "a", "e", "b", filterPredicate, projectionKeys, false);

    List<Embedding> filteredEdges = node.execute().collect();

    assertThat(filteredEdges.size(), is(1));
    assertThat(filteredEdges.get(0).getId(0).equals(sourceId), is(true));
    assertThat(filteredEdges.get(0).getId(1).equals(edge1Id), is(true));
    assertThat(filteredEdges.get(0).getId(2).equals(targetId), is(true));
  }
}
