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

import org.apache.commons.lang3.StringUtils;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Converts a GraphTransaction to the following .dot format:
 * <p>{@code
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
 *   }
 * </p>
 */
public class DotFileFormatSimple extends AbstractDotFileFormat {

  /**
   * Constructor
   *
   * @param printGraphHead true, iff graph head data shall be attached to the output
   */
  public DotFileFormatSimple(boolean printGraphHead) {
    setPrintGraphHead(printGraphHead);
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
      builder.append(VERTEX_ID_PREFIX)
        .append(vertex.getId())
        .append(suffix)
        .append(" [");

      writeLabel(builder, vertex);

      builder.append("];\n");
    }
  }

  @Override
  void writeLabel(StringBuilder builder, EPGMElement element) {
    String label = StringUtils.isEmpty(element.getLabel()) ? element.getId().toString() :
      element.getLabel();
    Properties properties = element.getProperties();

    if (properties != null && properties.size() > 0) {
      builder.append("label=\"").append(label).append("\"");

      for (Property property: properties) {
        builder.append(",")
          .append(property.getKey())
          .append("=\"")
          .append(property.getValue().toString())
          .append("\"");
      }
    } else {
      builder.append("label=\"").append(label).append("\"");
    }
  }
}
