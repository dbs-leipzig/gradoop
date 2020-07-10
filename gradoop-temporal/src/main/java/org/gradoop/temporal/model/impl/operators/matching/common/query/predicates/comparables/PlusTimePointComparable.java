/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;

/**
 * Wraps a
 */
public abstract class PlusTimePointComparable extends QueryComparable {

  /**
   * The wrapped PlusTimePoint
   *//*
    PlusTimePoint plusTimePoint;

    *//**
   * Creates a new wrapper
   *
   * @param plusTimePoint the wrapped PlusTimePoint
   *//*
    public PlusTimePointComparable(PlusTimePoint plusTimePoint) {
        this.plusTimePoint = plusTimePoint;
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData){
        if (plusTimePoint.getTimePoint() instanceof TimeLiteral) {
            return PropertyValue.create(plusTimePoint.evaluate());
        }
        if (plusTimePoint.getTimePoint() instanceof TimeSelector) {
            Long evalSelector =
                    new TimeSelectorComparable((TimeSelector) plusTimePoint.getTimePoint())
                            .evaluate(embedding, metaData).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalSelector + add);
        }
        if (plusTimePoint.getTimePoint() instanceof PlusTimePoint) {
            Long evalPlus =
                    new PlusTimePointComparable((PlusTimePoint) plusTimePoint.getTimePoint())
                            .evaluate(embedding, metaData).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalPlus + add);
        }
        return null;
    }

    @Override
    public PropertyValue evaluate(GraphElement element) {
        if (plusTimePoint.getTimePoint() instanceof TimeLiteral) {
            return PropertyValue.create(plusTimePoint.evaluate());
        }
        if (plusTimePoint.getTimePoint() instanceof TimeSelector) {
            Long evalSelector =
                    new TimeSelectorComparable((TimeSelector) plusTimePoint.getTimePoint())
                            .evaluate(element).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalSelector + add);
        }
        if (plusTimePoint.getTimePoint() instanceof PlusTimePoint) {
            Long evalPlus =
                    new PlusTimePointComparable((PlusTimePoint) plusTimePoint.getTimePoint())
                            .evaluate(element).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalPlus + add);
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys(String variable){
        return new HashSet<>(0);
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        PlusTimePointComparable that = (PlusTimePointComparable) o;

        return that.plusTimePoint.equals(plusTimePoint);
    }

    @Override
    public int hashCode(){
        return plusTimePoint != null ? plusTimePoint.hashCode() : 0;
    }
*/
}
