package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class EmbeddingTPGMMetaDataTest {
    @Test
    public void testFromHashMap() throws Exception {
        Map<Pair<String, EmbeddingTPGMMetaData.EntryType>, Integer> entryMap = new HashMap<>();
        entryMap.put(Pair.of("a", EmbeddingTPGMMetaData.EntryType.VERTEX), 0);
        entryMap.put(Pair.of("b", EmbeddingTPGMMetaData.EntryType.VERTEX), 1);
        entryMap.put(Pair.of("c", EmbeddingTPGMMetaData.EntryType.EDGE), 2);
        entryMap.put(Pair.of("d", EmbeddingMetaData.EntryType.PATH), 3);

        Map<Pair<String, String>, Integer> propertyMap = new HashMap<>();
        propertyMap.put(Pair.of("a", "age"), 0);
        propertyMap.put(Pair.of("b", "age"), 1);
        propertyMap.put(Pair.of("c", "since"), 2);

        Map<String, ExpandDirection> directionMap = new HashMap<>();
        directionMap.put("d", ExpandDirection.OUT);

        Map<String, Integer> timeDataMap = new HashMap<>();
        timeDataMap.put("a", 0);
        timeDataMap.put("b", 2);
        timeDataMap.put("c", 3);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData(
                entryMap, propertyMap, directionMap, timeDataMap);

        assertThat(metaData.getEntryCount(), is(4));
        assertThat(metaData.getEntryColumn("a"), is(0));
        assertThat(metaData.getEntryColumn("b"), is(1));
        assertThat(metaData.getEntryColumn("c"), is(2));
        assertThat(metaData.getEntryColumn("d"), is(3));
        assertThat(metaData.getEntryType("a"), is(EmbeddingTPGMMetaData.EntryType.VERTEX));
        assertThat(metaData.getEntryType("b"), is(EmbeddingTPGMMetaData.EntryType.VERTEX));
        assertThat(metaData.getEntryType("c"), is(EmbeddingTPGMMetaData.EntryType.EDGE));
        assertThat(metaData.getEntryType("d"), is(EmbeddingTPGMMetaData.EntryType.PATH));

        assertThat(metaData.getPropertyCount(), is(3));
        assertThat(metaData.getPropertyColumn("a", "age"), is(0));
        assertThat(metaData.getPropertyColumn("b", "age"), is(1));
        assertThat(metaData.getPropertyColumn("c", "since"), is(2));

        assertEquals(metaData.getTimeColumn("a"), 0);
        assertEquals(metaData.getTimeColumn("b"), 2);
        assertEquals(metaData.getTimeColumn("c"), 3);

        assertThat(metaData.getDirection("d"), is(ExpandDirection.OUT));

    }

    @Test
    public void testFromEmbeddingTPGMMetaData() throws Exception {
        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn("e", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        metaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        metaData.setEntryColumn("x", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        metaData.setEntryColumn("f", EmbeddingTPGMMetaData.EntryType.PATH, 3);
        metaData.setDirection("f", ExpandDirection.OUT);

        metaData.setPropertyColumn("a", "age", 0);

        metaData.setTimeColumn("a",0);
        metaData.setTimeColumn("b", 1);
        metaData.setTimeColumn("e", 2);
        metaData.setTimeColumn("x", 3);


        EmbeddingTPGMMetaData copy = new EmbeddingTPGMMetaData(metaData);
        assertThat(copy.getEntryColumn("a"), is(0));
        assertThat(copy.getEntryColumn("e"), is(1));
        assertThat(copy.getEntryColumn("b"), is(2));
        assertThat(copy.getEntryColumn("f"), is(3));
        assertThat(copy.getPropertyColumn("a", "age"), is(0));
        assertThat(copy.getDirection("f"), is(ExpandDirection.OUT));
        assertThat(copy.getTimeColumn("a"), is(0));
        assertThat(copy.getTimeColumn("b"), is(1));
        assertThat(copy.getTimeColumn("e"), is(2));
        assertThat(copy.getTimeColumn("x"), is(3));

        copy.setEntryColumn("c", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        assertThat(copy.getEntryCount(), is(6));
        assertThat(metaData.getEntryCount(), is(5));

        copy.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 5);
        assertThat(copy.getEntryColumn("a"), is(5));
        assertThat(metaData.getEntryColumn("a"), is(0));

        copy.setDirection("f", ExpandDirection.IN);
        assertThat(copy.getDirection("f"), is(ExpandDirection.IN));
        assertThat(metaData.getDirection("f"), is(ExpandDirection.OUT));

        copy.setTimeColumn("a",5);
        assertThat(copy.getTimeColumn("a"),is(5));
        assertThat(metaData.getTimeColumn("a"), is(0));
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetTimeColumnFailure(){
        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setTimeColumn("a",0);
        metaData.getTimeColumn("b");
    }

    @Test
    public void testEquals() throws Exception {
        EmbeddingTPGMMetaData metaData1 = new EmbeddingTPGMMetaData();
        metaData1.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData1.setPropertyColumn("a", "age", 0);
        metaData1.setTimeColumn("a",0);

        EmbeddingTPGMMetaData metaData2 = new EmbeddingTPGMMetaData();
        metaData2.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData2.setPropertyColumn("a", "age", 0);

        assertFalse(metaData1.equals(metaData2));
        assertFalse(metaData2.equals(metaData1));

        metaData2.setTimeColumn("a", 0);
        assertTrue(metaData1.equals(metaData2));
        assertTrue(metaData2.equals(metaData1));

        metaData1.setTimeColumn("a", 1);
        assertFalse(metaData1.equals(metaData2));
        assertFalse(metaData2.equals(metaData1));

        EmbeddingTPGMMetaData metaData3 = new EmbeddingTPGMMetaData();
        metaData3.setEntryColumn("b", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData3.setPropertyColumn("b", "age", 0);
        metaData3.setTimeColumn("b",0);
        assertFalse(metaData2.equals(metaData3));
        assertFalse(metaData3.equals(metaData2));

        metaData3.setTimeColumn("a",1);
        assertFalse(metaData1.equals(metaData3));
        assertFalse(metaData3.equals(metaData1));
    }

    @Test
    public void testHashCode() throws Exception {
        EmbeddingTPGMMetaData metaData1 = new EmbeddingTPGMMetaData();
        metaData1.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData1.setPropertyColumn("a", "age", 0);
        metaData1.setTimeColumn("a", 0);

        EmbeddingTPGMMetaData metaData2 = new EmbeddingTPGMMetaData();
        metaData2.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData2.setPropertyColumn("a", "age", 0);

        assertFalse(metaData1.hashCode() == metaData2.hashCode());

        metaData2.setTimeColumn("a", 0);
        assertTrue(metaData1.hashCode() == metaData2.hashCode());

        EmbeddingTPGMMetaData metaData3 = new EmbeddingTPGMMetaData();
        metaData3.setEntryColumn("b", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData3.setPropertyColumn("b", "age", 0);
        metaData3.setTimeColumn("b",0);

        assertFalse(metaData2.hashCode() == metaData3.hashCode());
    }

}
