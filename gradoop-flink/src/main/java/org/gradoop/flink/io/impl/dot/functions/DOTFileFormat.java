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
package org.gradoop.flink.io.impl.dot.functions;

import  static org.apache.commons.lang3.StringEscapeUtils.escapeHtml4;

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

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
   * id that needs to be update upon changes to this class' structure.
   */
  private static final long serialVersionUID = 1L;
  /**
   * .DOT vertex identifier prefix
   */
  private static final String VERTEX_ID_PREFIX = "v";
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

    GradoopId id = transaction.getGraphHead().getId();
    // writes for each graph:
    // digraph graphHeadId
    // {
    builder.append("subgraph cluster_g")
      .append(id)
      .append("{\n");

    //--------------------------------------------------------------------------
    // write DOT body
    //--------------------------------------------------------------------------

    if (printGraphHead) {
      writeGraphHead(transaction, builder);
    }

    writeVertices(transaction, builder, id.toString());

    writeEdges(transaction, builder, id.toString());

    //--------------------------------------------------------------------------
    // close DOT block
    //--------------------------------------------------------------------------

    builder.append("}\n");

    return builder.toString();
  }

  /**
   * Adds graph head information to the specified builder.
   *
   * Output: label="label";
   * @param transaction graph transaction
   * @param builder string builder to append
   */
  private void writeGraphHead(GraphTransaction transaction, StringBuilder builder) {
    GraphHead graphHead = transaction.getGraphHead();

    writeLabel(builder, graphHead, "#AAAAAA");

    builder.append(";\n");
  }

  /**
   * Adds vertex information to the specified builder.
   *
   * vertexId [label="label", property1="value1", ...];
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   * @param suffix id suffix specific for the current {@link GraphTransaction}
   */
  private void writeVertices(GraphTransaction transaction, StringBuilder builder, String suffix) {
    for (Vertex vertex: transaction.getVertices()) {
      // writes for each vertex:
      // "v1234",
      builder.append(VERTEX_ID_PREFIX)
        .append(vertex.getId())
        .append(suffix)
        .append(" [ shape=Mrecord, ");

      writeLabel(builder, vertex, "#000000");

      // writes:
      // ";"
      builder.append("];\n");
    }
  }

  /**
   * Adds edge information to the specified builder
   *
   * sourceId->targetId [label="label", property1="value1", ...];
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   * @param suffix id suffix specific for the current {@link GraphTransaction}
   */
  private void writeEdges(GraphTransaction transaction, StringBuilder builder, String suffix) {
    for (Edge edge: transaction.getEdges()) {

      builder.append(VERTEX_ID_PREFIX)
        .append(edge.getSourceId())
        .append(suffix)
        .append("->")
        .append(VERTEX_ID_PREFIX)
        .append(edge.getTargetId())
        .append(suffix)
        .append(" [");
      // write dot attributes if existent
      writeLabel(builder, edge, "#666666");
      builder.append("];\n");
    }
  }

  /**
   * Writes the specified label and properties as DOT HTML label string (table)
   *
   * output: label=<<table>...</table>>
   *
   * @param builder string builder to append
   * @param elem graph element with id, label and properties
   * @param color color for header background and properties text
   */
  private void writeLabel(StringBuilder builder, EPGMElement elem, String color) {
    String label = elem.getLabel();
    String id = elem.getId().toString();
    Properties properties = elem.getProperties();
    String lbl = StringUtils.isEmpty(label) ? id : label;

    if (properties != null && properties.size() > 0) {
      // writes properties as rows in html table
      //write white on black label/id as header
      builder.append("label=<")
        .append("<font color=\"").append(color).append("\">")
        .append("<table border=\"0\" cellborder=\"0\" cellpadding=\"3\">")
        .append("<tr><td colspan=\"2\" bgcolor=\"")
        .append(color)
        .append("\"><font color=\"white\">")
        .append(escapeHtml4(lbl))
        .append("</font></td></tr>");

      Iterator<Property> iterator = properties.iterator();
      while (iterator.hasNext()) {
        Property property = iterator.next();
        builder.append("<tr><td>")
          .append(escapeHtml4(property.getKey()))
          .append("</td><td>")
          .append(escapeHtml4(property.getValue().toString()))
          .append("</td></tr>");
      }
      builder.append("</table></font>>");
    } else {
      //write id/label as node label in dot
      builder.append("label=\"")
        .append(escapeHtml4(lbl))
        .append("\"");
    }
  }
}
