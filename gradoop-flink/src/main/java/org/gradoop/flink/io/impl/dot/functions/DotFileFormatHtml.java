/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import static org.apache.commons.lang3.StringEscapeUtils.escapeHtml4;

/**
 * Converts a GraphTransaction to the following .dot format:
 * <p>{@code
 *   digraph 0
 *   {
 *   gradoopId1 [label=<<table>...</table>>];
 *   gradoopId2 [label=<<table>...</table>>];
 *   gradoopID3;
 *   gradoopID4;
 *   gradoopId1->gradoopId2 [label=<<table>...</table>>];
 *   gradoopId2->gradoopId1 [label=<<table>...</table>>];
 *   gradoopId3->gradoopId4;
 *   }
 *   }
 * </p>
 */
public class DotFileFormatHtml extends AbstractDotFileFormat {

  /**
   * color color for header background and properties text
   */
  private String color;

  /**
   * Constructor
   *
   * @param printGraphHead true, iff graph head data shall be attached to the output
   * @param color the color for header background and properties text
   */
  public DotFileFormatHtml(boolean printGraphHead, String color) {
    setPrintGraphHead(printGraphHead);
    this.color = Preconditions.checkNotNull(color, "Color was null");
  }

  /**
   * Adds vertex information to the specified builder.
   *
   * {@code vertexId [label="label", property1="value1", ...];}
   *
   * @param transaction graph transaction
   * @param builder string builder to append
   * @param suffix id suffix specific for the current {@link GraphTransaction}
   */
  @Override
  void writeVertices(GraphTransaction transaction, StringBuilder builder, String suffix) {
    for (Vertex vertex: transaction.getVertices()) {
      // writes for each vertex:
      // "v1234",
      builder.append(VERTEX_ID_PREFIX)
        .append(vertex.getId())
        .append(suffix)
        .append(" [shape=Mrecord, ");

      writeLabel(builder, vertex);

      // writes:
      // ";"
      builder.append("];\n");
    }
  }

  /**
   * Writes the specified label and properties as DOT HTML label string (table)
   *
   * output: {@code label=<<table>...</table>>}
   *
   * @param builder string builder to append
   * @param elem graph element with id, label and properties
   */
  @Override
  void writeLabel(StringBuilder builder, EPGMElement elem) {
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

      for (Property property : properties) {
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
