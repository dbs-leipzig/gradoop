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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.api.entities.EPGMElement;
/**
 * EPGMElement with properties => properties
 *
 * @param <L> EPGMElement type having properties
 */
@FunctionAnnotation.ForwardedFields("properties->*")
public class Properties<L extends EPGMElement>
  implements MapFunction<L, org.gradoop.common.model.impl.properties.Properties>,
             KeySelector<L, org.gradoop.common.model.impl.properties.Properties> {

  @Override
  public org.gradoop.common.model.impl.properties.Properties map(L l) throws Exception {
    return l.getProperties();
  }

  @Override
  public org.gradoop.common.model.impl.properties.Properties getKey(L l) throws Exception {
    return l.getProperties();
  }
}
