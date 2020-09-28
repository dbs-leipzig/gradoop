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
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterAndProjectTemporalVerticesTest {

  public void testMetaDataInitialization() throws Exception {
    String variable = "a";
    FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(
      null, variable, new CNF(), Sets.newHashSet());

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertThat(embeddingMetaData.getEntryColumn(variable), is(0));
    assertThat(node.getEmbeddingMetaData().getPropertyKeys(variable).size(), is(0));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId vertex1Id = GradoopId.get();
    Map<String, Object> vertex1Props = new HashMap<>();
    vertex1Props.put("foo", 23);
    Long[] vertex1Time = new Long[] {123L, 1234L, 456L, 4567L};

    GradoopId vertex2Id = GradoopId.get();
    Map<String, Object> vertex2Props = new HashMap<>();
    vertex2Props.put("foo", 42);
    Long[] vertex2Time = new Long[] {9876L, 98765L, 654L, 65432L};

    GradoopId vertex3Id = GradoopId.get();
    Map<String, Object> vertex3Props = new HashMap<>();
    vertex2Props.put("foo", 23);
    Long[] vertex3Time = new Long[] {9876L, 98765L, 654L, 65432L};

    TemporalVertexFactory factory = new TemporalVertexFactory();

    TemporalVertex vertex1 = factory.createVertex("A", Properties.createFromMap(vertex1Props));
    vertex1.setId(vertex1Id);
    vertex1.setTransactionTime(new Tuple2<>(vertex1Time[0], vertex1Time[1]));
    vertex1.setValidTime(new Tuple2<>(vertex1Time[2], vertex1Time[3]));

    TemporalVertex vertex2 = factory.createVertex("B", Properties.createFromMap(vertex2Props));
    vertex2.setId(vertex2Id);
    vertex2.setTransactionTime(new Tuple2<>(vertex2Time[0], vertex2Time[1]));
    vertex2.setValidTime(new Tuple2<>(vertex2Time[2], vertex2Time[3]));

    TemporalVertex vertex3 = factory.createVertex("C", Properties.createFromMap(vertex3Props));
    vertex3.setId(vertex3Id);
    vertex3.setTransactionTime(new Tuple2<>(vertex3Time[0], vertex3Time[1]));
    vertex3.setValidTime(new Tuple2<>(vertex3Time[2], vertex3Time[3]));

    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(vertex1, vertex2, vertex3);

    String query = "MATCH (n) WHERE n.foo = 23 AND n.tx_from.before(Timestamp(1970-01-01T00:00:01)) " +
      "AND n.tx_to.before(Timestamp(2020-01-01))";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("n"));
    Set<String> projectionKeys = queryHandler.getPredicates().getPropertyKeys("n");

    FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(
      vertices, "n", filterPredicate, projectionKeys);
    List<Embedding> filteredVertices = node.execute().collect();

    assertThat(filteredVertices.size(), is(1));
    assertThat(filteredVertices.get(0).getId(0).equals(vertex1Id), is(true));


    EmbeddingMetaData metaData = node.getEmbeddingMetaData();
    assertEquals(metaData.getEntryColumn("n"), 0);
    System.out.println(metaData.getPropertyKeys("n"));

    int fooColumn = metaData.getPropertyColumn("n", "foo");
    int fromColumn = metaData.getPropertyColumn("n", TimeSelector.TimeField.TX_FROM.toString());
    int toColumn = metaData.getPropertyColumn("n", TimeSelector.TimeField.TX_TO.toString());
    assertTrue(fooColumn != fromColumn && fromColumn != toColumn && fooColumn != toColumn);
  }
}
