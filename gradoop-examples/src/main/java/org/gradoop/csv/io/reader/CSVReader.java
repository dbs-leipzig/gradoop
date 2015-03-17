package org.gradoop.csv.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.io.reader.ConfigurableVertexLineReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Reads csv input data
 */
public class CSVReader implements ConfigurableVertexLineReader {
  /**
   * The path to the meta_data of a csv file
   */
  public static final String META_DATA = "csv-reader.meta_data";
  /**
   * The type of a csv input (nodes or edges)
   */
  public static final String TYPE = "csv-reader.type";
  /**
   * The label (relationship) of a csv input
   */
  public static final String LABEL = "csv-reader.label";
  /**
   * The expected amount of nodes that will be created
   */
  public static final String EXPECTED_SIZE = "csv-reader.expected_size";
  /**
   * Default value of the expected size
   */
  public static final int DEFAULT_EXPECTED_SIZE = 0;
  /**
   * Token Separator of a csv line
   */
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("\\|");
  /**
   * Node type if csv input contains nodes
   */
  private static final String NODE_TYPE = "node";
  /**
   * Configuration
   */
  private Configuration conf;
  /**
   * Boolean operator for initial step
   */
  private boolean initialStep = true;
  /**
   * Contains the information about the csv type (if node its true)
   */
  private boolean isNodeCSV = false;
  /**
   * String List for vertex creation
   */
  private List<String> labels;
  /**
   * types of a csv file (e.g. long|string|string|integer)
   */
  private String[] types;
  /**
   * properties of a csv input (headline)
   */
  private String[] properties;
  /**
   * Random class declaration
   */
  private Random random;

  /**
   * Splits a line into tokens
   *
   * @param line line of csv input
   * @return tokens as array
   */
  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  /**
   * Initial step: initializations and reading the headline
   *
   * @param line line of csv input
   */
  private void initialStep(String line) {
    // Initialize Lists
    labels = Lists.newArrayList();
    // Get properties (e.g. FirstName, LastName...)
    properties = getTokens(line);
    // Get MetaData (e.g. long, string, string...)
    readTypes(conf.get(META_DATA));
    // Set Labels
    labels.add(conf.get(LABEL));
    // getCSVType
    isNodeCSV = isNodeCSV();
    random = new Random();
    // initialStep is over
    initialStep = false;
  }

  /**
   * Load Metadata Types e.g. long|string|string|...
   *
   * @param metaData configuration parameter
   */
  private void readTypes(String metaData) {
    try {
      BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(metaData),
          Charset.forName("UTF-8")));
      String line;
      while ((line = in.readLine()) != null) {
        types = getTokens(line);
      }
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Test the type of a CSV input (node or edge)
   *
   * @return true if node input
   */
  private boolean isNodeCSV() {
    return conf.get(TYPE).equals(NODE_TYPE);
  }

  /**
   * Sets the size of the initial vertex list
   *
   * @return vertex list
   */
  private List<Vertex> setVList() {
    int size = conf.getInt(EXPECTED_SIZE, DEFAULT_EXPECTED_SIZE);
    if (size == 0) {
      return Lists.newArrayList();
    } else {
      return Lists.newArrayListWithExpectedSize(size);
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(String line) {
    String[] tokens = getTokens(line);
    long id = Long.parseLong(tokens[0]);
    Vertex vex = VertexFactory.createDefaultVertexWithLabels(id, labels, null);
    for (int i = 1; i < properties.length; i++) {
      switch (types[i]) {
      case "long":
        vex.addProperty(properties[i], Long.parseLong(tokens[i]));
        break;
      case "string":
        vex.addProperty(properties[i], tokens[i]);
        break;
      case "integer":
        vex.addProperty(properties[i], Integer.parseInt(tokens[i]));
        break;
      default:
        vex.addProperty(properties[i], tokens[i]);
        break;
      }
    }
    return vex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Vertex> readVertexList(String line) {
    List<Vertex> vList = setVList();
    if (initialStep) {
      initialStep(line);
    } else {
      if (isNodeCSV) {
        vList.add(readVertex(line));
      } else {
        readEdges(vList, line);
      }
    }
    return vList;
  }

  /**
   * Creates vertices and edges
   *
   * @param vertices vertex list
   * @param line     line of csv input
   */
  private void readEdges(List<Vertex> vertices, String line) {
    List<String> label0 = Lists.newArrayListWithExpectedSize(1);
    List<String> label1 = Lists.newArrayListWithExpectedSize(1);
    String[] tokens = getTokens(line);
    long id0 = Long.parseLong(tokens[0]);
    long id1 = Long.parseLong(tokens[1]);
    String label = conf.get(LABEL);
    label0.add(properties[0].replace(".id", ""));
    label1.add(properties[1].replace(".id", ""));
    Edge outgoing =
      EdgeFactory.createDefaultEdgeWithLabel(id1, label, random.nextLong());
    Edge incoming =
      EdgeFactory.createDefaultEdgeWithLabel(id0, label, random.nextLong());
    for (int i = 2; i < properties.length; i++) {
      switch (types[i]) {
      case "long":
        outgoing.addProperty(properties[i], Long.parseLong(tokens[i]));
        incoming.addProperty(properties[i], Long.parseLong(tokens[i]));
        break;
      case "string":
        outgoing.addProperty(properties[i], tokens[i]);
        incoming.addProperty(properties[i], tokens[i]);
        break;
      case "integer":
        outgoing.addProperty(properties[i], Integer.parseInt(tokens[i]));
        incoming.addProperty(properties[i], Integer.parseInt(tokens[i]));
        break;
      default:
        outgoing.addProperty(properties[i], tokens[i]);
        incoming.addProperty(properties[i], tokens[i]);
        break;
      }
    }
    List<Edge> outgoingEdgeList = Lists.newArrayListWithExpectedSize(1);
    outgoingEdgeList.add(outgoing);
    List<Edge> incomingEdgeList = Lists.newArrayListWithCapacity(1);
    incomingEdgeList.add(incoming);
    Vertex vex0 = VertexFactory
      .createDefaultVertex(id0, label0, null, outgoingEdgeList, null, null);
    Vertex vex1 = VertexFactory
      .createDefaultVertex(id1, label1, null, null, incomingEdgeList, null);
    vertices.add(vex0);
    vertices.add(vex1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsVertexLists() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
