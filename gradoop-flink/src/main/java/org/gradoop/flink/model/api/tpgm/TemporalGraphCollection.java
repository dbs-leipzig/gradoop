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
package org.gradoop.flink.model.api.tpgm;

import org.gradoop.flink.model.api.layouts.TemporalLayout;

/**
 * A temporal (logical) graph collection is a base concept of the Temporal Property Graph Model
 * (TPGM) that extends the Extended Property Graph Model (EPGM). The temporal graph collection
 * inherits the main concepts of the {@link org.gradoop.flink.model.api.epgm.GraphCollection} and
 * extends them by temporal attributes. These attributes are two temporal information:
 * the valid-time and transaction time. Both are represented by a Tuple2 of Long values that specify
 * the beginning and end time as unix timestamp in milliseconds.
 *
 * transactionTime: (tx-from [ms], tx-to [ms])
 * validTime: (val-from [ms], val-to [ms])
 *
 * Furthermore, a temporal graph collection provides operations that are performed on the underlying
 * data. These operations result in either another temporal graph collection or in a
 * {@link TemporalGraph}.
 *
 * Analogous to a {@link org.gradoop.flink.model.api.epgm.GraphCollection}, a temporal graph
 * collection is wrapping a layout - in this case the {@link TemporalLayout} - which defines, how
 * the graph is represented in Apache Flink.
 * Note that the {@link TemporalGraphCollection} also implements that interface and just forward
 * the calls to the layout. This is just for convenience and API synchronicity.
 */
public interface TemporalGraphCollection extends TemporalLayout, TemporalGraphCollectionOperators {

}
