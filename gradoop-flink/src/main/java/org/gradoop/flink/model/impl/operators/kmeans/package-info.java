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

/**
 * A KMeans operator implementation takes a logical graph and assigns its vertices to a cluster.
 * The amount of clusters are user defined as well as the iterations and the spatial attributes. It
 * returns the logical graph and extends the properties of its vertices by a clusterId and the spatial
 * attributes of the cluster
 */
package org.gradoop.flink.model.impl.operators.kmeans;
