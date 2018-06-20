package org.gradoop.flink.io.impl.minimal;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class MinimalEdgeProvider {

  /**
   * Path to the edge csv file
   */
  private String edgeCsvPath;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Token delimiter
   */
  private static String tokenSeparator;

  /**
   * Name of the edge id column
   */
  private String edgeIdColumn;

  /**
   * Name of the source id column
   */
  private String sourceIdColumn;

  /**
   * Name of the target id column
   */
  private String targetIdColumn;

  /**
   * Name of the label column
   */
  private String edgeLabel;

  /**
   * Property names of all vertices
   */
  private static List<String> edgeProperties;
  
  public MinimalEdgeProvider(String edgeCsvPath, GradoopFlinkConfig config,
      String tokenSeperator, String edgeId, String sourceId, String targetId, String edgeLabel, List<String> edgeProperties) {

    this.edgeCsvPath = edgeCsvPath;
    this.config = config;
    this.tokenSeparator = tokenSeperator;
    this.edgeIdColumn = edgeId;
    this.sourceIdColumn = sourceId;
    this.targetIdColumn = targetId;
    this.edgeLabel = edgeLabel;
    this.edgeProperties = edgeProperties;
  }

public DataSet<ImportEdge<String>> importEdge() {
    
    return readCSVFile(config, edgeCsvPath, tokenSeparator);
  }
  
  public static DataSet<ImportEdge<String>> readCSVFile(
      GradoopFlinkConfig config, String edgeCsvPath, String tokenSeparator) {
    
    DataSet<ImportEdge<String>> lineTuples = config.getExecutionEnvironment()
        .readTextFile(edgeCsvPath)
        .map(line -> {
            String[] tokens = line.split(tokenSeparator, 5);
            ImportEdge<String> edges = map(tokens);
            return edges;
          }).returns(new TypeHint<ImportEdge<String>>() { });
    
    return lineTuples;
  }

  /**
   * Map a csv line to a EPMG edge
   * @param mapOperator external representation of the edge
   * @return EPMG edge
   */
  public static ImportEdge<String> map(String[] tokens) {

    return new ImportEdge<String>(tokens[0],
        tokens[1],
        tokens[2],
        tokens[3],
        parseProperties(tokens[4]));
  }

  /**
   * Map each label to the occurring properties.
   * @param label Name of the label
   * @param propertyValueString the properties
   * @return Properties as pojo element
   */
  public static Properties parseProperties(String propertyValueString) {

    Properties properties = new Properties();

    String[] propertyValues = propertyValueString.split(tokenSeparator);

    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(edgeProperties.get(i),
            PropertyValue.create(propertyValues[i]));
      }
    }
    return properties;
  }
}

