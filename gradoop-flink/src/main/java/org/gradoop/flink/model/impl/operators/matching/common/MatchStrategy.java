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
package org.gradoop.flink.model.impl.operators.matching.common;

/**
 * Used to select the strategy used by the matching algorithms
 */
public enum MatchStrategy {
    /**
     * If this strategy is used vertices and edges can only be
     * mapped to one vertices/edges in the query graph
     */
    ISOMORPHISM,
    /**
     * If this strategy is used vertices and edges can be
     * mapped to multiple vertices/edges in the query graph
     */
    HOMOMORPHISM
}
