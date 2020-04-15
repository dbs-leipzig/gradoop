package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FilterTemporalEmbeddingsTest extends PhysicalTPGMOperatorTest {
    @Test
    public void testFilterEmbeddings() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

        PropertyValue[] propertiesA = new PropertyValue[]{PropertyValue.create(23)};
        PropertyValue[] propertiesB = new PropertyValue[]{PropertyValue.create(42)};

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(GradoopId.get(), propertiesA);
        embedding.addTimeData(123L, 1234L, 345L, 3456L);
        embedding.add(GradoopId.get(), propertiesB);
        embedding.addTimeData(432L, 4321L, 543L, 5432L);

        DataSet<EmbeddingTPGM> embeddings = getExecutionEnvironment().fromElements(embedding);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 1);
        metaData.setPropertyColumn("a", "age", 0);
        metaData.setPropertyColumn("b", "age", 1);
        metaData.setTimeColumn("a", 0);
        metaData.setTimeColumn("b", 1);

        FilterTemporalEmbeddings filter = new FilterTemporalEmbeddings(embeddings, predicates, metaData);

        assertEquals(0, filter.evaluate().count());
    }

    @Test
    public void testKeepEmbeddings() throws Exception {
        CNF predicates = predicateFromQuery(
                "MATCH (a),(b) WHERE a.age > b.age AND a.tx.overlaps(b.val)");

        PropertyValue[] propertiesA = new PropertyValue[]{PropertyValue.create(42)};
        PropertyValue[] propertiesB = new PropertyValue[]{PropertyValue.create(23)};

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(GradoopId.get(), propertiesA);
        embedding.addTimeData(0L, 1000L, 345L, 3456L);
        embedding.add(GradoopId.get(), propertiesB);
        embedding.addTimeData(0L, 1000L, 500L, 1500L);

        DataSet<EmbeddingTPGM> embeddings = getExecutionEnvironment().fromElements(embedding);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 1);
        metaData.setPropertyColumn("a", "age", 0);
        metaData.setPropertyColumn("b", "age", 1);
        metaData.setTimeColumn("a",0);
        metaData.setTimeColumn("b",1);

        FilterTemporalEmbeddings filter = new FilterTemporalEmbeddings(embeddings, predicates, metaData);

        assertEquals(1, filter.evaluate().count());
    }

    @Test
    public void testDontAlterEmbeddingTPGM() throws Exception {
        CNF predicates = predicateFromQuery(
                "MATCH (a),(b) WHERE a.age > b.age AND a.tx.overlaps(b.val)");

        PropertyValue[] propertiesA = new PropertyValue[]{PropertyValue.create(42)};
        PropertyValue[] propertiesB = new PropertyValue[]{PropertyValue.create(23)};

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(GradoopId.get(), propertiesA);
        embedding.addTimeData(0L, 1000L, 345L, 3456L);
        embedding.add(GradoopId.get(), propertiesB);
        embedding.addTimeData(0L, 1000L, 500L, 1500L);

        DataSet<EmbeddingTPGM> embeddings = getExecutionEnvironment().fromElements(embedding);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 1);
        metaData.setPropertyColumn("a", "age", 0);
        metaData.setPropertyColumn("b", "age", 1);
        metaData.setTimeColumn("a",0);
        metaData.setTimeColumn("b",1);

        FilterTemporalEmbeddings filter = new FilterTemporalEmbeddings(embeddings, predicates, metaData);

        assertEquals(embedding, filter.evaluate().collect().get(0));
    }
}
