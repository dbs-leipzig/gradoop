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
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

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
public class DOTFileFormat
  implements TextOutputFormat.TextFormatter<GraphTransaction> {
  /**
   * Whitespace
   */
  private static final String WHITESPACE = " ";
  /**
   * .DOT header string
   */
  private static final String DOT_DIGRAPH_HEADER = "digraph";
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
  private boolean graphInformation;

  /**
   * Constructor
   *
   * @param graphInformation flag to print graph head information
   */
  public DOTFileFormat(Boolean graphInformation) {
    this.graphInformation = graphInformation;
  }

  @Override
  public String format(GraphTransaction transaction) {

    StringBuilder builder = new StringBuilder();

    //--------------------------------------------------------------------------
    // write dot head lines and open block
    //--------------------------------------------------------------------------

    // remove "-" from GradoopId (reserved character in dot format)
    String graphHeadId = transaction.getGraphHead()
      .getId().toString().replace("-", "");

    // writes for each graph:
    // digraph graphHeadId
    // {
    builder.append(String.format("%s%s%s%n%s%n",
      DOT_DIGRAPH_HEADER,
      WHITESPACE,
      graphHeadId,
      DOT_BLOCK_OPEN));

    //--------------------------------------------------------------------------
    // write graph information (optional)
    // graph [label="label", property1="value1", ...];
    //--------------------------------------------------------------------------

    if (graphInformation) {
      GraphHead graphHead = transaction.getGraphHead();

      // writes:
      // "graph"
      builder.append(String.format("%s",
        DOT_GRAPH_TAG));

      // write dot attributes if existent
      if (!graphHead.getLabel().equals(GConstants.DEFAULT_GRAPH_LABEL) ||
        graphHead.getProperties().size() > 0) {

        builder.append(writeDOTAttributes(
          graphHead.getLabel(), graphHead.getProperties()));
      }

      // writes:
      // ";"
      builder.append(String.format("%s%n",
        DOT_LINE_ENDING));
    }

    //--------------------------------------------------------------------------
    // write vertex lines
    // vertexId [label="label", property1="value1", ...];
    //--------------------------------------------------------------------------

    for (Vertex vertex: transaction.getVertices()) {

      // remove "-" from GradoopId (reserved character in dot format)
      String vertexId = vertex.getId().toString().replace("-", "");

      // writes for each vertex:
      // "vertexId",
      builder.append(String.format("%s",
        vertexId));

      // write dot attributes if existent
      if (!vertex.getLabel().equals(GConstants.DEFAULT_VERTEX_LABEL) ||
        vertex.getProperties().size() > 0) {

        builder.append(writeDOTAttributes(
          vertex.getLabel(), vertex.getProperties()));
      }

      // writes:
      // ";"
      builder.append(String.format("%s%n",
        DOT_LINE_ENDING));
    }

    //--------------------------------------------------------------------------
    // write edge lines
    // sourceId->targetId [label="label", property1="value1", ...];
    //--------------------------------------------------------------------------

    for (Edge edge: transaction.getEdges()) {

      // remove "-" from GradoopId (reserved character in dot format)
      String sourceId = edge.getSourceId().toString().replace("-", "");
      String targetId = edge.getTargetId().toString().replace("-", "");

      // writes for each edge:
      // "sourceId->targetId"
      builder.append(String.format("%s%s%s",
        sourceId,
        DOT_OUT_EDGE,
        targetId));

      // write dot attributes if existent
      if (!edge.getLabel().equals(GConstants.DEFAULT_EDGE_LABEL) ||
        edge.getProperties().size() > 0) {
        builder.append(writeDOTAttributes(
          edge.getLabel(), edge.getProperties()));
      }

      // writes:
      // ";"
      builder.append(String.format("%s%n",
        DOT_LINE_ENDING));
    }

    //--------------------------------------------------------------------------
    // close dot block
    //--------------------------------------------------------------------------

    builder.append(String.format("%s", DOT_BLOCK_CLOSE));

    return builder.toString();
  }

  /**
   * Writes all attributes of the epgm element as string
   *
   * @param label         label of the epgm element
   * @param propertyList  List of properties
   * @return              properties as string
   */
  private String writeDOTAttributes(String label, PropertyList propertyList) {

    StringBuilder attributeBuilder = new StringBuilder();

    // write:
    // " ["
    attributeBuilder.append(String.format("%s%s",
      WHITESPACE,
      DOT_ATTRIBUTES_OPEN));

    // writes:
    // "label="label""
    if (!label.equals(GConstants.DEFAULT_GRAPH_LABEL)) {
      attributeBuilder.append(String.format("%s%s%s",
        DOT_LABEL_TAG,
        label,
        DOT_ATTRIBUTE_CLOSE));
    }

    // writes for each property:
    // ",propertyKey1=propertyValue1,propertyKey2=propertyValue2,..."
    if (propertyList.size() > 0) {
      for (Property property : propertyList) {
        attributeBuilder.append(String.format("%s%s%s%s%s",
          DOT_ATTRIBUTE_SEPARATOR,
          property.getKey(),
          DOT_ATTRIBUTE_OPEN,
          property.getValue(),
          DOT_ATTRIBUTE_CLOSE));
      }
    }

    // writes:
    // "]"
    attributeBuilder.append(String.format("%s",
      DOT_ATTRIBUTES_CLOSE));

    return attributeBuilder.toString();
  }
}
