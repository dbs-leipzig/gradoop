package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary.FilterTemporalEmbeddingsNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class FilterTemporalEmbeddingsNodeTest {
    @Test
    public void testMetaDataInitialization() throws Exception {
        EmbeddingTPGMMetaData mockMeta = new EmbeddingTPGMMetaData();
        mockMeta.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        mockMeta.setPropertyColumn("a", "age", 0);
        mockMeta.setTimeColumn("a", 0);
        PlanNode mockNode = new MockPlanNode(null, mockMeta);

        FilterTemporalEmbeddingsNode node = new FilterTemporalEmbeddingsNode(mockNode, new TemporalCNF());

        assertTrue(mockNode.getEmbeddingMetaData().equals(node.getEmbeddingMetaData()));
    }

    @Test
    public void testExecute() throws Exception {
        String query = "MATCH (a)-->(b) WHERE a.age > b.age AND a.tx_from.before(b.tx_from)" +
                " AND a.tx_to.before(2020-01-01)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);
        TemporalCNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("a", "b"));

        GradoopId vertexAId = GradoopId.get();
        GradoopId vertexBId = GradoopId.get();
        GradoopId vertexCId = GradoopId.get();
        GradoopId vertexDId = GradoopId.get();

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 1);
        metaData.setPropertyColumn("a", "age", 0);
        metaData.setPropertyColumn("b", "age", 1);
        metaData.setTimeColumn("a", 0);
        metaData.setTimeColumn("b", 1);

        EmbeddingTPGM embedding1 = new EmbeddingTPGM();
        embedding1.add(vertexAId, PropertyValue.create(42));
        embedding1.add(vertexBId, PropertyValue.create(23));
        embedding1.addTimeData(1L, 2L, 3L, 4L);
        embedding1.addTimeData(10L, 20L, 30L, 40L);

        EmbeddingTPGM embedding2 = new EmbeddingTPGM();
        embedding2.add(vertexAId, PropertyValue.create(42));
        embedding2.add(vertexCId, PropertyValue.create(84));
        embedding2.addTimeData(1L, 2L, 3L, 4L);
        embedding2.addTimeData(10L, 20L, 30L, 40L);

        EmbeddingTPGM embedding3 = new EmbeddingTPGM();
        embedding3.add(vertexAId, PropertyValue.create(42));
        embedding3.add(vertexCId, PropertyValue.create(41));
        embedding3.addTimeData(10L, 200L, 3L, 4L);
        embedding3.addTimeData(1L, 20L, 30L, 40L);

        DataSet<EmbeddingTPGM> input = getExecutionEnvironment().fromElements(embedding1, embedding2, embedding3);
        PlanNode mockChild = new MockPlanNode(input, metaData);

        List<EmbeddingTPGM> result = new FilterTemporalEmbeddingsNode(mockChild, filterPredicate).execute().collect();

        assertThat(result.size(), is(1));
        assertTrue(result.get(0).getId(0).equals(vertexAId));
        assertTrue(result.get(0).getId(1).equals(vertexBId));
        assertTrue(result.get(0).getProperty(0).equals(PropertyValue.create(42)));
        assertTrue(result.get(0).getProperty(1).equals(PropertyValue.create(23)));
        assertArrayEquals(result.get(0).getTimes(0), new Long[]{1L, 2L, 3L, 4L});
        assertArrayEquals(result.get(0).getTimes(1), new Long[]{10L, 20L, 30L, 40L});

        assertEquals(new FilterTemporalEmbeddingsNode(mockChild, filterPredicate).getEmbeddingMetaData(), metaData);
    }
}
