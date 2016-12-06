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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.PathEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public abstract class PhysicalOperatorTest extends GradoopFlinkTestBase {

  protected void assertEmbeddingExists(DataSet<Embedding> dataSet, GradoopId... path)
    throws Exception {
    List<GradoopId> pathList = Lists.newArrayList(path);
    assertTrue(
      dataSet.collect()
        .stream()
        .anyMatch(embedding -> pathList.equals(embeddingToIdList(embedding)))
    );
  }

  protected void assertEmbeddingExists(DataSet<Embedding> dataSet, Predicate<Embedding> predicate)
    throws Exception {

    assertTrue(dataSet.collect().stream().anyMatch(predicate::test));
  }

  protected void assertEveryEmbedding(DataSet<Embedding> dataSet, Consumer<Embedding> consumer)
    throws Exception {

    dataSet.collect().forEach(consumer);
  }

  protected Embedding createEmbedding(GradoopId... ids) {
    Embedding embedding = new Embedding();

    for (GradoopId id : ids) {
      embedding.addEntry(new IdEntry(id));
    }

    return embedding;
  }

  protected DataSet<Embedding> createEmbeddings(int size, GradoopId... idEntries) {
    return createEmbeddings(size, (EmbeddingEntry[]) Arrays.stream(idEntries).map(IdEntry::new).toArray());
  }

  protected DataSet<Embedding> createEmbeddings(Integer size, EmbeddingEntry... entries) {
    List<Embedding> embeddings = new ArrayList<>(size);

    ArrayList<EmbeddingEntry> embeddingEntries = Lists.newArrayList(entries);

    for (int i = 0; i < size; i++) {
      embeddings.add(new Embedding(embeddingEntries));
    }

    return getExecutionEnvironment().fromCollection(embeddings);
  }

  protected DataSet<Vertex> createVerticesWithProperties(List<String> propertyNames) {
    Properties properties = getProperties(propertyNames);
    VertexFactory vertexFactory = new VertexFactory();

    List<Vertex> vertices = Lists.newArrayList(
      vertexFactory.createVertex("Label1",properties),
      vertexFactory.createVertex("Label2",properties)
    );

    return getExecutionEnvironment().fromCollection(vertices);
  }

  protected DataSet<Edge> createEdgesWithProperties(List<String> propertyNames) {
    Properties properties = getProperties(propertyNames);
    EdgeFactory edgeFactory = new EdgeFactory();

    List<Edge> edges = Lists.newArrayList(
      edgeFactory.createEdge("Label1", GradoopId.get(), GradoopId.get(), properties),
      edgeFactory.createEdge("Label2", GradoopId.get(), GradoopId.get(), properties)
    );

    return getExecutionEnvironment().fromCollection(edges);
  }

  protected Properties getProperties(List<String> propertyNames) {
    Properties properties = new Properties();

    for(String property_name : propertyNames) {
      properties.set(property_name, property_name);
    }

    return properties;
  }

  protected CNF predicateFromQuery(String query) {
    return new QueryHandler(query).getPredicates();
  }

  private List<GradoopId> embeddingToIdList(Embedding embedding) {
    return embedding.getEntries().stream().flatMap(entry -> {
      if(entry instanceof PathEntry) {
        return ((PathEntry) entry).getPath().stream();
      }
      return Lists.newArrayList(entry.getId()).stream();
    }).collect(Collectors.toList());
  }
}
