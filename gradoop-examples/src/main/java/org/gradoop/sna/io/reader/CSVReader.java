/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.sna.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.io.reader.ConfigurableVertexLineReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;

import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Reads sna input data
 */
public class CSVReader implements ConfigurableVertexLineReader {
  /**
   * The path to the meta_data of a sna file
   */
  public static final String META_DATA = "sna-reader.meta_data";
  /**
   * The type of a sna input (nodes or edges)
   */
  public static final String TYPE = "sna-reader.type";
  /**
   * The label (relationship) of a sna input
   */
  public static final String LABEL = "sna-reader.label";
  /**
   * CSV Properties
   */
  public static final String PROPERTIES = "sna-reader.properties";
  /**
   * The expected amount of nodes that will be created
   */
  public static final String EXPECTED_SIZE = "sna-reader.expected_size";
  /**
   * Default value of the expected size
   */
  public static final int DEFAULT_EXPECTED_SIZE = 0;
  /**
   * Token Separator of a sna line
   */
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("\\|");
  /**
   * Node type if sna input contains nodes
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
   * Contains the information about the sna type (if node its true)
   */
  private boolean isNodeCSV = false;
  /**
   * String List for vertex creation
   */
  private String labels;
  /**
   * types of a sna file (e.g. long|string|string|integer)
   */
  private String[] types;
  /**
   * Properties of a sna input (headline)
   */
  private String[] properties;
  /**
   * Random class declaration
   */
  private Random random;

  /**
   * Splits a line into tokens
   *
   * @param line line of sna input
   * @return tokens as array
   */
  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  /**
   * Initial step: initializations and reading the headline
   */
  private void initialStep() {
    // Initialize Lists
    labels = "";
    // Get properties (e.g. FirstName, LastName...)
    properties = getTokens(conf.get(PROPERTIES));
    // Get MetaData (e.g. long, string, string...)
    types = getTokens(conf.get(META_DATA));
    //readTypes(conf.get(META_DATA));
    // Set Labels
    labels = conf.get(LABEL);
    // getCSVType
    isNodeCSV = isNodeCSV();
    random = new Random();
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
    Vertex vex = VertexFactory.createDefaultVertexWithLabel(id, labels, null);
    for (int i = 1; i < properties.length; i++) {
      switch (types[i]) {
      case "long":
        vex.setProperty(properties[i], Long.parseLong(tokens[i]));
        break;
      case "string":
        vex.setProperty(properties[i], tokens[i]);
        break;
      case "integer":
        vex.setProperty(properties[i], Integer.parseInt(tokens[i]));
        break;
      default:
        vex.setProperty(properties[i], tokens[i]);
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
      initialStep();
      initialStep = false;
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
   * @param line     line of sna input
   */
  private void readEdges(List<Vertex> vertices, String line) {
    String[] tokens = getTokens(line);
    long id0 = Long.parseLong(tokens[0]);
    long id1 = Long.parseLong(tokens[1]);
    String edgeLabel = conf.get(LABEL);
    String nodeLabel0 = properties[0].replace(".id", "");
    String nodeLabel1 = properties[1].replace(".id", "");
    Edge outgoing =
      EdgeFactory.createDefaultEdgeWithLabel(id1, edgeLabel, random.nextLong());
    Edge incoming =
      EdgeFactory.createDefaultEdgeWithLabel(id0, edgeLabel, random.nextLong());
    if (properties.length > 2) {
      for (int i = 2; i < properties.length; i++) {
        switch (types[i]) {
        case "long":
          outgoing.setProperty(properties[i], Long.parseLong(tokens[i]));
          incoming.setProperty(properties[i], Long.parseLong(tokens[i]));
          break;
        case "string":
          outgoing.setProperty(properties[i], tokens[i]);
          incoming.setProperty(properties[i], tokens[i]);
          break;
        case "integer":
          outgoing.setProperty(properties[i], Integer.parseInt(tokens[i]));
          incoming.setProperty(properties[i], Integer.parseInt(tokens[i]));
          break;
        default:
          outgoing.setProperty(properties[i], tokens[i]);
          incoming.setProperty(properties[i], tokens[i]);
          break;
        }
      }
    }
    List<Edge> outgoingEdgeList = Lists.newArrayListWithExpectedSize(1);
    outgoingEdgeList.add(outgoing);
    List<Edge> incomingEdgeList = Lists.newArrayListWithCapacity(1);
    incomingEdgeList.add(incoming);
    Vertex vex0 = VertexFactory
      .createDefaultVertex(id0, nodeLabel0, null, outgoingEdgeList, null, null);
    Vertex vex1 = VertexFactory
      .createDefaultVertex(id1, nodeLabel1, null, null, incomingEdgeList, null);
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
