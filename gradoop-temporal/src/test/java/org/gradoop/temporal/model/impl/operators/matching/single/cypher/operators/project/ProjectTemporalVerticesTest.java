package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.ProjectVertices;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import java.util.ArrayList;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEveryEmbeddingTPGM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProjectTemporalVerticesTest extends PhysicalTPGMOperatorTest {
    @Test
    public void returnsEmbeddingWithOneProjection() throws Exception {
        TemporalVertexFactory factory = new TemporalVertexFactory();
        ArrayList<String> propertyNames = Lists.newArrayList("foo", "bar", "baz");
        TemporalVertex v1 = factory.createVertex("l1", getProperties(propertyNames));
        v1.setTransactionTime(new Tuple2<>(1L, 2L));
        v1.setValidTime(new Tuple2<>(3L, 4L));
        TemporalVertex v2 = factory.createVertex("l2", getProperties(propertyNames));
        v2.setTransactionTime(new Tuple2<>(5L, 6L));
        v2.setValidTime(new Tuple2<>(7L, 8L));

        DataSet<TemporalVertex> edgeDataSet = getExecutionEnvironment().fromElements(v1, v2);

        ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "baz");

        ProjectTemporalVertices operator = new ProjectTemporalVertices(edgeDataSet, extractedPropertyKeys);
        DataSet<EmbeddingTPGM> results = operator.evaluate();

        assertEquals(2, results.count());

        assertEveryEmbeddingTPGM(results, (embedding) -> {
            assertEquals(1, embedding.size());
            assertEquals(PropertyValue.create("foo"), embedding.getProperty(0));
            assertEquals(PropertyValue.create("baz"), embedding.getProperty(1));
        });
        assertArrayEquals(results.collect().get(0).getTimes(0), new Long[]{1L, 2L, 3L, 4L});
        assertArrayEquals(results.collect().get(1).getTimes(0), new Long[]{5L, 6L, 7L, 8L});
    }
}
