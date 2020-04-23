package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class stores meta data information about a data set of {@link EmbeddingTPGM} objects.
 *
 * An {@link EmbeddingTPGM} stores identifiers (single or path), properties associated with query
 * elements and temporal data for each edge or vertex (tx_from, tx_to, valid_from, valid_to)
 *
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData}
 * contains a mapping between query variables and the column index storing the
 * associated element/path identifier. Furthermore, a
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData}
 * object contains a mapping between property values associated to property keys at query elements.
 * This class adds a mapping between query variables and the column index storing their temporal
 * data (i.e. 4 longs tx_from, tx_to, valid_from, valid_to)
 */
public class EmbeddingTPGMMetaData extends
        org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData {
    /**
     * Stores the mapping of query variables to the column containing their time data
     */
    private Map<String, Integer> timeDataMapping;

    /**
     * Initializes an empty EmbeddingTPGMMetaData object
     */
    public EmbeddingTPGMMetaData(){
        super();
        timeDataMapping = new HashMap<>();
    }

    /**
     * Initializes a new EmbeddingTPGMMetaData object from the given mappings
     *
     * @param entryMapping maps variables to embedding entries
     * @param propertyMapping maps variable-propertyKey pairs to embedding property data entries
     * @param directionMapping maps (path) variables to their direction
     * @param timeDataMapping maps variables to time data entries
     */
    public EmbeddingTPGMMetaData(Map<Pair<String, EntryType>, Integer> entryMapping,
                                 Map<Pair<String, String>, Integer> propertyMapping,
                                 Map<String, ExpandDirection> directionMapping,
                                 Map<String, Integer> timeDataMapping){
        super(entryMapping, propertyMapping, directionMapping);
        this.timeDataMapping = timeDataMapping;
    }

    /**
     * Initializes a new EmbeddingTPGMMetaData object using copies of the provided meta data.
     *
     * @param metaData meta data to be copied
     */
    public EmbeddingTPGMMetaData(EmbeddingTPGMMetaData metaData){
        super(metaData);
        this.timeDataMapping = new HashMap<>();
        Set<String> timeVariables = metaData.getTimeDataMapping().keySet();
        metaData.getVariables().forEach(var -> {
            if(metaData.getEntryType(var)!=EntryType.PATH) {
                if(timeVariables.contains(var)) {
                    this.timeDataMapping.put(var, metaData.getTimeColumn(var));
                }
            }
        });

    }

    public Map<String, Integer> getTimeDataMapping(){
        return timeDataMapping;
    }

    /**
     * Inserts or updates a mapping from a variable name to the column containing its time data
     *
     * @param var referenced variable
     * @param column corresponding time data index
     */
    public void setTimeColumn(String var, int column){
        timeDataMapping.put(var, column);
    }

    /**
     * Returns the position of the time data entry corresponding to the given variable.
     *
     * @param var variable name
     * @return position of the corresponding time data entry (time data index)
     * @throws NoSuchElementException if there is no time data entry corresponding to the variable
     */
    public int getTimeColumn(String var){
        try {
            return timeDataMapping.get(var);
        }
        catch(NullPointerException e){
            throw new NoSuchElementException();
        }
    }

    /**
     * Number of variables for which time data is stored
     * @return number of variables for which time data is stored
     */
    public int getTimeCount(){
        return timeDataMapping.size();
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmbeddingTPGMMetaData metaData = (EmbeddingTPGMMetaData) o;

        return getEntryMapping().equals(metaData.getEntryMapping()) &&
                getPropertyMapping().equals(metaData.getPropertyMapping()) &&
                timeDataMapping.equals(metaData.getTimeDataMapping());
    }

    @Override
    public int hashCode(){
        int result = getPropertyMapping().hashCode();
        result = 31 * result + getEntryMapping().hashCode();
        result = 31 * result + getTimeDataMapping().hashCode();
        return result;
    }

    @Override
    public String toString(){
        List<Map.Entry<Pair<String, EntryType>, Integer>> sortedEntries = getEntryMapping().entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList());

        List<Map.Entry<Pair<String, String>, Integer>> sortedProperties = getPropertyMapping().entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList());

        List<Map.Entry<String, Integer>> sortedTimeData = timeDataMapping.entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList());


        return String.format("EmbeddingMetaData{entryMapping=%s, propertyMapping=%s, timeDataMapping=%s}",
                sortedEntries, sortedProperties, sortedTimeData);
    }

}
