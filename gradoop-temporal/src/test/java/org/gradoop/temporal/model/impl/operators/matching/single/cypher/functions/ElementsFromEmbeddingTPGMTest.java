package org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.id.GradoopId;

import org.gradoop.common.model.impl.properties.PropertyValue;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;

import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ElementsFromEmbeddingTPGMTest {

    static Long[] defaultTime = new Long[]{TemporalElement.DEFAULT_TIME_FROM, TemporalElement.DEFAULT_TIME_TO,
            TemporalElement.DEFAULT_TIME_FROM, TemporalElement.DEFAULT_TIME_TO};
    static TemporalGraphHeadFactory ghFactory = new TemporalGraphHeadFactory();
    static TemporalVertexFactory vFactory = new TemporalVertexFactory();
    static TemporalEdgeFactory eFactory = new TemporalEdgeFactory();

    @Test
    public void testSimpleElementFromEmbeddingTPGM() throws Exception {
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();

        //------------------------------------
        // Vertices
        //------------------------------------
        //ids
        GradoopId v1 = GradoopId.get();
        GradoopId v2 = GradoopId.get();
        GradoopId v3 = GradoopId.get();
        //times
        Long[] v1Time = getRandomTime();
        Long[] v2Time = getRandomTime();
        Long[] v3Time = getRandomTime();

        //insert into embedding and metadata
        embedding.add(v1);
        embedding.add(v2);
        embedding.add(v3);

        for(Long[] time: new Long[][]{v1Time, v2Time, v3Time}){
            embedding.addTimeData(time[0], time[1], time[2], time[3]);
        }

        metaData.setEntryColumn("v1", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn("v2", EmbeddingMetaData.EntryType.VERTEX, 1);
        metaData.setEntryColumn("v3", EmbeddingMetaData.EntryType.VERTEX, 2);
        metaData.setTimeColumn("v1", 0);
        metaData.setTimeColumn("v2", 1);
        metaData.setTimeColumn("v3", 2);



        //------------------------------------
        // Edges/Paths
        //------------------------------------
        //ids
        GradoopId e1 = GradoopId.get();
        //times
        Long[] e1Time = getRandomTime();

        //labels

        //insert into embedding and metadata
        embedding.add(e1);
        //path
        embedding.addTimeData(e1Time[0], e1Time[1], e1Time[2], e1Time[3]);


        metaData.setEntryColumn("e1", EmbeddingMetaData.EntryType.EDGE, 3);

        metaData.setTimeColumn("e1", 3);
        metaData.setDirection("e1", ExpandDirection.OUT);


        HashMap<String, Pair<String, String>> sourceTargetVariables = new HashMap<>();
        sourceTargetVariables.put("e1", Pair.of("v1", "v2"));

        HashMap<String, String> labelMapping = new HashMap<>();
        labelMapping.put("v1", "v1Label");
        labelMapping.put("v2", "v2Label");
        //labelMapping.put("v3", "v3Label");
        labelMapping.put("e1", "e1Label");

        DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment()
                .fromElements(embedding);

        List<Element> result = input.flatMap(
                new ElementsFromEmbeddingTPGM<TemporalGraphHead,
                        TemporalVertex, TemporalEdge>(ghFactory, vFactory, eFactory,
                        metaData, sourceTargetVariables, labelMapping)).collect();


        checkElement((TemporalVertex) result.get(0), v1, "v1Label", v1Time);
        checkElement((TemporalVertex) result.get(1), v2, "v2Label", v2Time);
        checkElement((TemporalVertex) result.get(2), v3, null, v3Time);


        checkElement((TemporalEdge) result.get(3), e1, "e1Label", e1Time);
    }

    @Test
    public void testPathsFromEmbeddingTPGM() throws Exception {
        GradoopId source = GradoopId.get();
        GradoopId target = GradoopId.get();
        Long[] sourceTime = getRandomTime();
        Long[] targetTime = getRandomTime();

        GradoopId e1 = GradoopId.get();
        GradoopId v = GradoopId.get();
        GradoopId e2 = GradoopId.get();
        GradoopId w = GradoopId.get();
        GradoopId e3 = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(source);
        embedding.add(e1, v, e2, w, e3);
        embedding.add(target);

        embedding.addTimeData(sourceTime[0], sourceTime[1], sourceTime[2], sourceTime[3]);
        embedding.addTimeData(targetTime[0], targetTime[1], targetTime[2], targetTime[3]);


        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("source", EmbeddingMetaData.EntryType.VERTEX,0);
        metaData.setEntryColumn("p", EmbeddingMetaData.EntryType.PATH, 1);
        metaData.setEntryColumn("target", EmbeddingMetaData.EntryType.VERTEX, 2);

        metaData.setTimeColumn("source", 0);
        metaData.setTimeColumn("target", 1);

        metaData.setDirection("p", ExpandDirection.OUT);

        HashMap<String, String> labelMapping = new HashMap<>();
        labelMapping.put("source", "sourceLabel");
        labelMapping.put("target", "targetLabel");

        HashMap<String, Pair<String, String>> sourceTargetVariables = new HashMap<>();
        sourceTargetVariables.put("p", Pair.of("source", "target"));

        DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment()
                .fromElements(embedding);

        List<Element> result = input.flatMap(
                new ElementsFromEmbeddingTPGM<TemporalGraphHead,
                        TemporalVertex, TemporalEdge>(ghFactory, vFactory, eFactory,
                        metaData, sourceTargetVariables, labelMapping)).collect();

        // 7 elements + graph head
        assertEquals(result.size(), 8);
        checkElement((TemporalVertex) result.get(0), source, "sourceLabel", sourceTime);
        checkElement((TemporalVertex) result.get(1), target, "targetLabel", targetTime);
        Map<PropertyValue, PropertyValue> variableMapping = result.get(7).
                getPropertyValue("__variable_mapping").getMap();
        List<PropertyValue> pValues = variableMapping.get(PropertyValue.create("p"))
                .getList();
        System.out.println(pValues);
        assertEquals(pValues.get(0).getGradoopId(), e1);
        assertEquals(pValues.get(1).getGradoopId(), v);
        assertEquals(pValues.get(2).getGradoopId(), e2);
        assertEquals(pValues.get(3).getGradoopId(), w);
        assertEquals(pValues.get(4).getGradoopId(), e3);
    }

    private void checkElement(TemporalElement v, GradoopId id, String label, Long[] time){
        assertEquals(v.getLabel(), label);
        assertEquals(v.getTransactionTime(), new Tuple2<>(time[0], time[1]));
        assertEquals(v.getValidTime(), new Tuple2<>(time[2], time[3]));
        assertEquals(v.getId(), id);
    }

    private Long[] getRandomTime(){
        int min = 0;
        int max = 1000;
        Long tx_from = (long) (Math.random()*max);
        Long tx_to = tx_from + ((long)(Math.random()*max));
        Long valid_from = (long) (Math.random()*max);
        Long valid_to = valid_from + ((long)(Math.random()*max));
        return new Long[]{tx_from, tx_to, valid_from, valid_to};
    }
}
