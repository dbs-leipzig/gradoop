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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.List;

/**
 * RichMapFunction that maps edge tuples to rows.
 * A new row field is created for each property that edges
 * with this label can have.
 */

@FunctionAnnotation.ForwardedFields("f0->f0;f1->f1;f2->f2")
public class TupleToRow extends RichMapFunction<Tuple5<Long, Long, Long, String, Properties>, Row> {

  /**
   * List of all property keys the edges with this label have.
   */
  private List<String> propertyKeys;

  /**
   * Reduce object instantiations
   */
  private Row returnRow;

  /**
   * Constructor
   *
   * @param propertyKeys list of all property keys edges with this label have
   */
  public TupleToRow(List<String> propertyKeys) {
    this.propertyKeys = propertyKeys;
    returnRow = new Row(propertyKeys.size() + 3);
  }

  @Override
  public Row map(Tuple5<Long, Long, Long, String, Properties> tuple) throws Exception {

    returnRow.setField(0, tuple.f0);
    returnRow.setField(1, tuple.f1);
    returnRow.setField(2, tuple.f2);

    int i = 3;
    for (String key : propertyKeys) {
      if (tuple.f4.containsKey(key)) {
        returnRow.setField(i, tuple.f4.get(key).getObject());
      }
      i++;
    }
    return returnRow;
  }
}
