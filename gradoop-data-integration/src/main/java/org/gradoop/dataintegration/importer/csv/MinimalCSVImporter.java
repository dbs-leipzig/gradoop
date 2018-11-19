/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.dataintegration.importer.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Read a csv file and import each row as a vertex in EPGM representation.
 *
 */
public class MinimalCSVImporter {
    
    /**
     * Token delimiter
     */
    private String tokenSeparator;
    
    /**
     * Path to the csv file
     */
    private String path;
    
    /**
     * Gradoop Flink configuration
     */
     private GradoopFlinkConfig config;
    
    public MinimalCSVImporter(String path, String tokenSeperator, GradoopFlinkConfig config) {
        this.path = path;
        this.tokenSeparator = tokenSeperator;
        this.config = config;
    }
    
    /**
     * Import each row of the file as a vertex.
     * @return the imported vertices
     */
    public DataSet<ImportVertex<Long>> importVertices() {
        return readCSVFile(readHeaderRow());
    }
    
    /**
     * Read the vertices from a csv file.
     *
     * @param config Gradoop Flink configuration
     * @param vertexCsvPath path to the file
     * @param tokenSeparator separator
     * @return DateSet of all vertices from one specific file.
     */
    public DataSet<ImportVertex<Long>> readCSVFile(ArrayList<String> propertyNames) {
        
        DataSet<Properties> lines = config.getExecutionEnvironment()
        .readTextFile(path)
        .map(new RowToVertexMapper(path, tokenSeparator, propertyNames))
        .filter(new FilterNullValuesTuple<Properties>());
        
        return DataSetUtils.zipWithUniqueId(lines).map(new CreateImportVertexCSV<>());
    }
    
    /**
     * Read the fist row of a csv file and put each the entry in each column in a list.
     * @return the property names
     */
    public ArrayList<String> readHeaderRow() {
        try ( final BufferedReader reader =
                new BufferedReader(new InputStreamReader(new URL(path).openStream(),
                        "UTF-8"))) { //use UTF-8 as charset
            String headerLine = reader.readLine();
            headerLine = headerLine.substring(0, headerLine.length() - 1); //remove the row delimiter from String
            String[] headerArray;
            headerArray = headerLine.split(tokenSeparator);
            
            return new ArrayList<>(Arrays.asList(headerArray));
            
        } catch (IOException ex) {
            return null;
        }
    }
}
