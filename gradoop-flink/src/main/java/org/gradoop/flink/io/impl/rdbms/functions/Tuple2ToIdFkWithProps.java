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

package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.Fk1Fk2Props;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

/**
 * Creates tuples of gradoop id (fk1), key value (fk2) and belonging properties
 */
public class Tuple2ToIdFkWithProps implements
    MapFunction<Tuple2<Fk1Fk2Props, IdKeyTuple>, Tuple3<GradoopId, String, Properties>> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Tuple3<GradoopId, String, Properties> map(Tuple2<Fk1Fk2Props, IdKeyTuple> value)
      throws Exception {
    return new Tuple3<GradoopId, String, Properties>(value.f1.f0, value.f0.f1, value.f0.f2);
  }
}
