package org.gradoop.flink.io.impl.minimal;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class MinimalVertexProvider {

  /**
   * Path to the vertex csv file
   */
  private String vertexCsvPath;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Token delimiter
   */
  private static String tokenSeparator;

  /**
   * Name of the id column
   */
  private String vertexIdColumn;

  /**
   * Name of the label column
   */
  private String vertexLabel;

  /**
   * Property names of all vertices
   */
  private static List<String> vertexProperties;
  
  /**
   * Construktor.
   *
   * @param vertexPath Path to the vertex file
   * @param config Gradoop Flink configuration
   * @param tokenSeparator Delimiter of csv file
   * @param vertexIdColumn Name of the id column
   * @param labelColumn Name of the label column
   * @param vertexProperties List of all property names
   */
  public MinimalVertexProvider(String vertexPath, GradoopFlinkConfig config,
      String tokenSeparator, String vertexIdColumn, String labelColumn,
      List<String> vertexProperties) {
    this.vertexCsvPath = vertexPath;
    this.config = config;
    this.tokenSeparator = tokenSeparator;
    this.vertexIdColumn = vertexIdColumn;
    this.vertexLabel = labelColumn;
    this.vertexProperties = vertexProperties;
  }
  
  public DataSet<ImportVertex<String>> importVertex() {
    
    return readCSVFile(config, vertexCsvPath, tokenSeparator);
  }
  
  public static DataSet<ImportVertex<String>> readCSVFile(
      GradoopFlinkConfig config, String vertexCsvPath, String tokenSeparator) {
    
    DataSet<ImportVertex<String>> lineTuples = config.getExecutionEnvironment()
        .readTextFile(vertexCsvPath)
        .map(line -> {
            String[] tokens = line.split(tokenSeparator, 3);
            ImportVertex<String> vertices = map(tokens);
            return vertices;
          }).returns(new TypeHint<ImportVertex<String>>() { });
    
    return lineTuples;
  }

  /**
   * Map a csv line to a EPMG vertex
   * @param mapOperator external representation of the vertex
   * @return EPMG vertex
   */
  public static ImportVertex<String> map(String[] tokens) {

    return new ImportVertex<String>(tokens[0],
        tokens[1],
        parseProperties(tokens[2]));
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
        properties.set(vertexProperties.get(i),
            PropertyValue.create(propertyValues[i]));
      }
    }
    return properties;
  }
}
