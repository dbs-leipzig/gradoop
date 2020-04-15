package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractPropertyJoinColumns;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Given a set of property columns and one of time selectors, this key selector
 * returns a concatenated string of the specified property columns and the time selectors
 * It concatenates the output of a {@link ExtractPropertyJoinColumns} with the output of a
 * {@link ExtractTimeJoinColumns}.
 */
public class ExtractValueJoinColumns implements KeySelector<EmbeddingTPGM, String> {
    /**
     * Property columns to concatenate properties from
     */
    private final List<Integer> properties;
    /**
     * Time selectors to concatenate time data from
     * Time Selectors are of the form (a,b) where a denotes the time column and 0<=b<=3 the
     * time value to select: 0=tx_from, 1=tx_to, 2=valid_from, 3=valid_to
     */
    private final List<Tuple2<Integer, Integer>> timeData;
    /**
     * Stores the concatenated key string
     */
    private final StringBuilder sb;
    /**
     * separates the property string from the time string
     */
    private final String SEP = "~";

    /**
     * Creates the key selector
     *
     * @param properties columns to create hash code from
     */
    public ExtractValueJoinColumns(List<Integer> properties,
                                      List<Tuple2<Integer, Integer>> timeData) {
        this.properties = properties;
        this.timeData = timeData;
        this.sb = new StringBuilder();
    }

    @Override
    public String getKey(EmbeddingTPGM value) throws Exception {
        sb.delete(0, sb.length());

        sb.append(new ExtractPropertyJoinColumns(properties).getKey(value));
        sb.append(SEP);
        sb.append(new ExtractTimeJoinColumns(timeData).getKey(value));

        return sb.toString();
    }
}
