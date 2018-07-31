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
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.junit.Test;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static org.junit.Assert.*;

public class ElementsFromEmbeddingTest {

  @Test
  public void variableMappingTest() throws Exception{
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId v3 = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId e2 = GradoopId.get();

    QueryHandler query = new QueryHandler("(a)-[e]->(b)-[f]->(c)");

    Traverser traverser = new DFSTraverser();
    traverser.setQueryHandler(query);

    TraversalCode traversalCode = traverser.traverse();

    ElementsFromEmbedding udf = new ElementsFromEmbedding(
      traversalCode, new GraphHeadFactory(), new VertexFactory(), new EdgeFactory(), query
    );
    udf.open(new Configuration());

    GradoopId[] vertexMapping = new GradoopId[]{v1,v2,v3};
    GradoopId[] edgeMapping = new GradoopId[]{e1,e2};

    Embedding<GradoopId> embedding = new Embedding<>();
    embedding.setVertexMapping(vertexMapping);
    embedding.setEdgeMapping(edgeMapping);

    List<Element> result = new ArrayList<>();

    udf.flatMap(Tuple1.of(embedding), new ListCollector<>(result));

    GraphHead graphHead = (GraphHead) result
      .stream()
      .filter(e -> e instanceof GraphHead)
      .findFirst()
      .get();

    assertTrue(graphHead.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));

    Map<PropertyValue, PropertyValue> variableMapping
      = graphHead.getPropertyValue(PatternMatching.VARIABLE_MAPPING_KEY).getMap();

    for (Vertex queryVertex : query.getVertices()) {
      assertEquals(
        variableMapping.get(PropertyValue.create(queryVertex.getVariable())),
        PropertyValue.create(vertexMapping[(int) queryVertex.getId()])
      );
    }

    for (Edge queryEdge : query.getEdges()) {
      assertEquals(
        variableMapping.get(PropertyValue.create(queryEdge.getVariable())),
        PropertyValue.create(edgeMapping[(int) queryEdge.getId()])
      );
    }
  }
}