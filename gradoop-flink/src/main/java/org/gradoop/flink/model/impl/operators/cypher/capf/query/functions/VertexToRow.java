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
package org.gradoop.flink.model.impl.operators.cypher.capf.query.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 * RichMapFunction that maps tuples containing a Long id and an vertex to row.
 * A new row field is created for each property that vertices
 * with this label can have.
 */

@FunctionAnnotation.ForwardedFields("f0->f0")
@FunctionAnnotation.ReadFields("f1.properties")
public class VertexToRow extends RichMapFunction<Tuple2<Long, Vertex>, Row> {

  /**
   * List of all property keys the edges with this label have.
   */
  private Collection<String> propertyKeys;

  /**
   * Reduce object instantiations
   */
  private Row returnRow;

  /**
   * Constructor
   *
   * @param propertyKeys list of all property keys edges with this label have
   */
  public VertexToRow(Collection<String> propertyKeys) {
    this.propertyKeys = propertyKeys;
    returnRow = new Row(propertyKeys.size() + 1);
  }

  @Override
  public Row map(Tuple2<Long, Vertex> tuple) throws Exception {
    returnRow.setField(0, tuple.f0);

    int i = 1;
    for (String key : propertyKeys) {
      if (tuple.f1.hasProperty(key)) {
        returnRow.setField(i, tuple.f1.getPropertyValue(key).getObject());
      }
      i++;
    }
    return returnRow;
  }
}
