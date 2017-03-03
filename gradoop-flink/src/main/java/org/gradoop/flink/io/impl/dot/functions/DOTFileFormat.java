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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.dot.functions;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Iterator;

/**
 * Converts a GraphTransaction to the following .dot format:
 * <p>
 *   digraph 0
 *   {
 *   gradoopId1 [label="person",name="Bob",age="20",...];
 *   gradoopId2 [label="person",name="Alice",age="20",...];
 *   gradoopID3;
 *   gradoopID4;
 *   gradoopId1->gradoopId2 [label="knows",since="2003",...];
 *   gradoopId2->gradoopId1 [label="knows",since="2003",...];
 *   gradoopId3->gradoopId4;
 *   }
 * </p>
 */
public class DOTFileFormat implements TextOutputFormat.TextFormatter<GraphTransaction> {
  /**
   * Whitespace
   */
  private static final String WHITESPACE = " ";
  /**
   * .DOT header string
   */
  private static final String DOT_DIGRAPH_HEADER = "digraph";
  /**
   * .DOT graph identifier prefix
   */
  private static final String GRAPH_ID_PREFIX = "g";
  /**
   * .DOT vertex identifier prefix
   */
  private static final String VERTEX_ID_PREFIX = "v";
  /**
   * .DOT block open string
   */
  private static final String DOT_BLOCK_OPEN = "{";
  /**
   * .DOT graph block open
   */
  private static final String DOT_GRAPH_TAG = "graph";
  /**
   * .DOT block close string
   */
  private static final String DOT_BLOCK_CLOSE = "}";
  /**
   * .DOT directed edge string
   */
  private static final String DOT_OUT_EDGE = "->";
  /**
   * .DOT attributes open string
   */
  private static final String DOT_ATTRIBUTES_OPEN = "[";
  /**
   * .DOT label open tag
   */
  private static final String DOT_LABEL_TAG = "label=\"";
  /**
   * .DOT attributes close string
   */
  private static final String DOT_ATTRIBUTES_CLOSE = "]";
  /**
   * .DOT line ending string
   */
  private static final String DOT_LINE_ENDING = ";";
  /**
   * .DOT attribute separator string
   */
  private static final String DOT_ATTRIBUTE_SEPARATOR = ",";
  /**
   * .DOT attribute open string
   */
  private static final String DOT_ATTRIBUTE_OPEN = "=\"";
  /**
   * .DOT attribute close string
   */
  private static final String DOT_ATTRIBUTE_CLOSE = "\"";
  /**
   * flag to print graph head information to dot
   */
  private boolean printGraphHead;

  /**
   * Constructor
   *
   * @param printGraphHead true, iff graph head data shall be attached to the output
   */
  public DOTFileFormat(Boolean printGraphHead) {
    this.printGraphHead = printGraphHead;
  }

  @Override
  public String format(GraphTransaction transaction) {

    StringBuilder builder = new StringBuilder();

    //--------------------------------------------------------------------------
    // write DOT head lines and open block
    //--------------------------------------------------------------------------

    // writes for each graph:
    // digraph graphHeadId
    // {
    builder.append(String.format("%s%s%s%n%s%n",
      DOT_DIGRAPH_HEADER,
      WHITESPACE,
      GRAPH_ID_PREFIX + transaction.getGraphHead().getId(),
      DOT_BLOCK_OPEN));

    //--------------------------------------------------------------------------
    // write DOT body
    //--------------------------------------------------------------------------

    if (printGraphHead) {
      writeGraphHead(transaction, builder);
    }

    writeVertices(transaction, builder);

    writeEdges(transaction, builder);

    //--------------------------------------------------------------------------
    // close DOT block
    //--------------------------------------------------------------------------

    builder.append(String.format("%s", DOT_BLOCK_CLOSE));

    return builder.toString();
  }

  /**
   * Adds graph head information to the specified builder.
   *
   * Output: graph [label="label", property1="value1", ...];
   * @param transaction graph transaction
   * @param builder string builder to append
   */
  private void writeGraphHead(GraphTransaction transaction, StringBuilder builder) {
    GraphHead graphHead = transaction.getGraphHead();

    // writes:
    // "graph"
    builder.append(String.format("%s", DOT_GRAPH_TAG));

    writeDOTAttributes(builder, graphHead.getLabel(), graphHead.getProperties());

    // writes:
    // ";"
    builder.append(String.format("%s%n", DOT_LINE_ENDING));
  }

  /**
   * Adds vertex information to the specified builder.
   *
   * vertexId [label="label", property1="value1", ...];
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   */
  private void writeVertices(GraphTransaction transaction, StringBuilder builder) {
    for (Vertex vertex: transaction.getVertices()) {
      // writes for each vertex:
      // "v1234",
      builder.append(String.format("%s%s", VERTEX_ID_PREFIX, vertex.getId()));

      writeDOTAttributes(builder, vertex.getLabel(), vertex.getProperties());

      // writes:
      // ";"
      builder.append(String.format("%s%n", DOT_LINE_ENDING));
    }
  }

  /**
   * Adds edge information to the specified builder
   *
   * sourceId->targetId [label="label", property1="value1", ...];
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   */
  private void writeEdges(GraphTransaction transaction, StringBuilder builder) {
    for (Edge edge: transaction.getEdges()) {

      String sourceId = VERTEX_ID_PREFIX + edge.getSourceId();
      String targetId = VERTEX_ID_PREFIX + edge.getTargetId();

      // writes for each edge:
      // "sourceId->targetId"
      builder.append(String.format("%s%s%s", sourceId, DOT_OUT_EDGE, targetId));

      // write dot attributes if existent
      writeDOTAttributes(builder, edge.getLabel(), edge.getProperties());

      // writes:
      // ";"
      builder.append(String.format("%s%n", DOT_LINE_ENDING));
    }
  }

  /**
   * Writes the specified label and properties as DOT attribute string
   *
   * output: ["label"="label","propertyKey1=propertyValue1,propertyKey2=propertyValue2,...]
   *
   * @param builder string builder to append
   * @param label label
   * @param properties properties
   */
  private void writeDOTAttributes(StringBuilder builder, String label, Properties properties) {
    // write:
    // " ["
    builder.append(String.format("%s%s", WHITESPACE, DOT_ATTRIBUTES_OPEN));

    // writes:
    // "label="label""
    if (!label.isEmpty()) {
      builder.append(String.format("%s%s%s", DOT_LABEL_TAG, label, DOT_ATTRIBUTE_CLOSE));
    }

    // writes:
    // "propertyKey1=propertyValue1,propertyKey2=propertyValue2,..."
    if (properties != null && properties.size() > 0) {
      if (!label.isEmpty()) {
        builder.append(DOT_ATTRIBUTE_SEPARATOR);
      }
      Iterator<Property> iterator = properties.iterator();
      while (iterator.hasNext()) {
        Property property = iterator.next();
        builder.append(String.format("%s%s%s%s",
          property.getKey(),
          DOT_ATTRIBUTE_OPEN,
          property.getValue(),
          DOT_ATTRIBUTE_CLOSE));
        if (iterator.hasNext()) {
          builder.append(DOT_ATTRIBUTE_SEPARATOR);
        }
      }
    }

    // writes:
    // "]"
    builder.append(String.format("%s", DOT_ATTRIBUTES_CLOSE));
  }
}
