/**
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
package org.gradoop.common.storage.api;

import org.gradoop.common.config.GradoopStoreConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * The EPGM store is responsible for writing and reading graph heads, vertices
 * and edges.
 *
 * @param <C>  StoreConfig type
 * @param <IG> EPGM graph head type (input)
 * @param <IV> EPGM vertex type (input)
 * @param <IE> EPGM edge type (input)
 * @param <OG> EPGM graph head type (output)
 * @param <OV> EPGM vertex type (output)
 * @param <OE> EPGM edge type (output)
 */
public interface EPGMStore<C extends GradoopStoreConfig,
  IG extends EPGMGraphHead,
  IV extends EPGMVertex,
  IE extends EPGMEdge,
  OG extends EPGMGraphHead,
  OV extends EPGMVertex,
  OE extends EPGMEdge> extends
  EPGMConfigProvider<C>, EPGMGraphInput<IG, IV, IE>, EPGMGraphOutput<OG, OV, OE> {

}
