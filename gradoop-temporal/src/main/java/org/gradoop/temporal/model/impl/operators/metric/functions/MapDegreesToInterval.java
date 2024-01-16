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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * A map partition function that calculates the intervals in which the values stay the same
 * by checking if the current value is the same as the previous one
 *
 */

public class MapDegreesToInterval implements MapPartitionFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {
    @Override
    public void mapPartition(Iterable<Tuple2<Long, Double>> values, Collector<Tuple3<Long, Long, Double>> out) {

        //set starting values to null
        Long startTimestamp = null;
        Long endTimestamp = null;
        Double value = null;
        Boolean collected = false;

        //loop through each tuple
        for (Tuple2<Long, Double> tuple : values) {
            if (startTimestamp == null) {
                // First element in the group
                startTimestamp = tuple.f0;
                endTimestamp = tuple.f0;
                value = tuple.f1;
            } else {
                if (!tuple.f1.equals(value)) {
                    // Value changed, emit the current interval and start a new one
                    out.collect(new Tuple3<>(startTimestamp, tuple.f0, value));
                    startTimestamp = tuple.f0;
                    endTimestamp = tuple.f0;
                    value = tuple.f1;
                    collected = true;
                } else {
                    // Extend the current interval
                    endTimestamp = tuple.f0;
                    collected = false;
                }
            }
        }
        //check if the latest interval was collected, if not, collect it
        //this happens when the last interval has the value 0
        if (!collected) {
            out.collect(new Tuple3<>(startTimestamp, endTimestamp, value));
        }
    }
}