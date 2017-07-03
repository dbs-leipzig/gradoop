
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.ArrayList;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.assertEveryEmbedding;
import static org.junit.Assert.assertEquals;

public class ProjectVerticesTest extends PhysicalOperatorTest {

  @Test
  public void returnsEmbeddingWithOneProjection() throws Exception{
    DataSet<Vertex> edgeDataSet = createVerticesWithProperties(
      Lists.newArrayList("foo", "bar","baz")
    );

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "baz");

    ProjectVertices operator = new ProjectVertices(edgeDataSet, extractedPropertyKeys);
    DataSet<Embedding> results = operator.evaluate();

    assertEquals(2,results.count());

    assertEveryEmbedding(results, (embedding) -> {
      assertEquals(1, embedding.size());
      assertEquals(PropertyValue.create("foo"), embedding.getProperty(0));
      assertEquals(PropertyValue.create("baz"), embedding.getProperty(1));
    });
  }
}
