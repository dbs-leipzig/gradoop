/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.layouts.transactional;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.GraphCollectionLayoutFactoryTest;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TxCollectionLayoutFactoryTest extends GraphCollectionLayoutFactoryTest {
  @Override
  protected GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> getFactory() {
    TxCollectionLayoutFactory txCollectionLayoutFactory = new TxCollectionLayoutFactory();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    config.setGraphCollectionLayoutFactory(txCollectionLayoutFactory);
    return txCollectionLayoutFactory;
  }
}
