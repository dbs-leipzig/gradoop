package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractPropertyJoinColumns;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExtractValueJoinColumnsTest {

    @Test
    public void emptyTest(){
        EmbeddingTPGM e = new EmbeddingTPGM();
        e.add(GradoopId.get(), new PropertyValue[]{PropertyValue.create("test")},
                1L, 2L,3L, 4L);

        List<Tuple2<Integer, Integer>> timeData = new ArrayList<>();
        List<Integer> properties = new ArrayList<>();

        // only the separator
        String expected = "~";
        String actual = "";
        try {
            actual = new ExtractValueJoinColumns(properties, timeData).getKey(e);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        assertEquals(expected, actual);

    }

    @Test
    public void emptyPropertyTest(){
        EmbeddingTPGM e = new EmbeddingTPGM();
        e.add(GradoopId.get(), new PropertyValue[]{PropertyValue.create("test")},
                1L, 2L,3L, 4L);

        List<Tuple2<Integer, Integer>> timeData = new ArrayList<>();
        timeData.add(new Tuple2<>(0,1));
        List<Integer> properties = new ArrayList<>();

        // separator + time
        String expected = null;
        try {
            expected = "~"+new ExtractTimeJoinColumns(timeData).getKey(e);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        String actual = "";
        try {
            actual = new ExtractValueJoinColumns(properties, timeData).getKey(e);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        assertEquals(expected, actual);
    }

    @Test
    public void emptyTimeTest(){
        EmbeddingTPGM e = new EmbeddingTPGM();
        e.add(GradoopId.get(), new PropertyValue[]{PropertyValue.create("test")},
                1L, 2L,3L, 4L);

        List<Tuple2<Integer, Integer>> timeData = new ArrayList<>();
        List<Integer> properties = new ArrayList<>();
        properties.add(0);

        // properties + separator
        String expected = null;
        try {
            expected = new ExtractPropertyJoinColumns(properties).getKey(e)+ "~";
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        String actual = "";
        try {
            actual = new ExtractValueJoinColumns(properties, timeData).getKey(e);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        assertEquals(expected, actual);
    }

    @Test
    public void nonEmptyTest(){
        EmbeddingTPGM e = new EmbeddingTPGM();
        e.add(GradoopId.get(), new PropertyValue[]{PropertyValue.create("test")},
                1L, 2L,3L, 4L);

        List<Tuple2<Integer, Integer>> timeData = new ArrayList<>();
        timeData.add(new Tuple2<>(0,1));
        List<Integer> properties = new ArrayList<>();
        properties.add(0);

        // properties + separator + timedata
        String expected = null;
        try {
            expected = new ExtractPropertyJoinColumns(properties).getKey(e)+ "~" +
            new ExtractTimeJoinColumns(timeData).getKey(e);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        String actual = "";
        try {
            actual = new ExtractValueJoinColumns(properties, timeData).getKey(e);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        assertEquals(expected, actual);
    }
}
