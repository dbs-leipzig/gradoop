package org.gradoop.dataintegration.importer.csv;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class RowToVertexMapper implements MapFunction<String, Properties>{

    /**
     * Token separator for the csv file.
     */
    private String tokenSperator;
    
    /**
     * The path to the csv file
     */
    private String filePath;
    
    /**
     * The name of the properties
     */
    private ArrayList<String> propertyNames;
    
    public RowToVertexMapper(String filePath, String tokenDelimiter, ArrayList<String> propertyNames) {
        this.filePath = filePath;
        this.tokenSperator = tokenDelimiter;
        this.propertyNames = propertyNames;
    }
    
    @Override
    public Properties map(final String line) throws Exception {
        
        return parseProperties(line, propertyNames);
    }

    /**
     * Map each label to the occurring properties.
     * @param line one row of the csv file, contains all the property values of one vertex
     * @param propertyNames identifier of the property values
     * @return the properties of the vertex
     */
    private Properties parseProperties(String line, ArrayList<String> propertyNames) {
        
        Properties properties = new Properties();
        
        String[] propertyValues = line.split(tokenSperator);
        
        /*
         * If the line to read is equals to the header, we do not import this line
         * (because the file contains a header line, which is not a vertex).
         * In this case, we return null, else we return the line tuple.
         */
        boolean equals = false;
        if (propertyValues.length == propertyNames.size()) {
            for (int i = 0; i < propertyValues.length; i++) {
                if (propertyValues[i].trim().equals(propertyNames.get(i).trim())) {
                    equals = true;
                } else {
                    equals = false;
                    break;
                }
            }
            if (equals) {
                return null;
            }
        }
        
        for (int i = 0; i < propertyValues.length; i++) {
            if (propertyValues[i].length() > 0) {
                properties.set(propertyNames.get(i),
                        PropertyValue.create(propertyValues[i]));
            }
        }
        return properties;
    }
}
