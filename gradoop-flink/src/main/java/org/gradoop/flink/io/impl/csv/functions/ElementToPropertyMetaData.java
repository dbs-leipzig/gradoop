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
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;

import java.util.HashSet;
import java.util.Set;

/**
 * (element) -> (elementType, elementLabel, {key_1:type_1,key_2:type_2,...,key_n:type_n})
 *
 * @param <E> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class ElementToPropertyMetaData<E extends Element>
  implements MapFunction<E, Tuple3<String, String, Set<String>>> {
  /**
   * Reduce object instantiations.
   */
  private final Tuple3<String, String, Set<String>> reuseTuple;
  /**
   * Constructor
   */
  public ElementToPropertyMetaData() {
    reuseTuple = new Tuple3<>();
    reuseTuple.f2 = new HashSet<>();
  }

  @Override
  public Tuple3<String, String, Set<String>> map(E e) throws Exception {
    Class<? extends Element> type = e.getClass();

    if (type == Edge.class) {
      reuseTuple.f0 = CSVConstants.EDGE_TYPE;
    } else if (type == Vertex.class) {
      reuseTuple.f0 = CSVConstants.VERTEX_TYPE;
    } else if (type == GraphHead.class) {
      reuseTuple.f0 = CSVConstants.GRAPH_TYPE;
    } else {
      throw new Exception("Unsupported element class");
    }
    reuseTuple.f1 = StringEscaper.escape(e.getLabel(), CSVConstants.ESCAPED_CHARACTERS);
    reuseTuple.f2.clear();
    if (e.getProperties() != null) {
      for (Property property : e.getProperties()) {
        reuseTuple.f2.add(MetaDataParser.getPropertyMetaData(property));
      }
    }
    return reuseTuple;
  }
}
