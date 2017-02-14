/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.functions.tuple.Project2To1;
import org.gradoop.flink.model.impl.operators.join.functions.StandardStringConcatenation;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

import java.util.HashSet;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class JoinUtils {

  public static <K,V> KeySelector<Tuple2<K,V>,V> keySelectorFromRightProjection(Project2To1<K, V>
    p) {
    return new KeySelector<Tuple2<K,V>, V>() {
      @Override
      public V getKey(Tuple2<K,V> value) throws Exception {
        return p.map(value).f0;
      }
    };
  }

  public static <K,V> KeySelector<K, V> functionToKeySelector(final Function<K,V> fkv) {
    return new KeySelector<K, V>() {
      @Override
      public V getKey(K value) throws Exception {
        return fkv.apply(value);
      }
    };
  }


  public static <K,V> MapFunction<K,V> functionToMapFunction(final Function<K,V> mop) {
    return new MapFunction<K, V>() {
      @Override
      public V map(K value) throws Exception {
        return mop.apply(value);
      }
    };
  }

  public static <K> JoinOperatorSetsBase<K, K> joinByType(DataSet<K> left, DataSet<K> right, JoinType
    joinType) {
    switch (joinType) {
    case INNER:
      return left.join(right);
    case LEFT_OUTER:
      return left.leftOuterJoin(right);
    case RIGHT_OUTER:
      return left.rightOuterJoin(right);
    default:
      return left.fullOuterJoin(right);
    }
  }

  public static <K,J> JoinOperatorSetsBase<K, J> joinByVertexEdge(DataSet<K> left, DataSet<J>
    right,
    JoinType
    joinType, boolean isCorrectOrder) {
    switch (joinType) {
    case INNER:
      return left.join(right);
    case LEFT_OUTER:
      return isCorrectOrder ? left.leftOuterJoin(right) : left.join(right);
    case RIGHT_OUTER:
      return isCorrectOrder ? left.join(right) : left.leftOuterJoin(right);
    default:
      return left.fullOuterJoin(right);
    }
  }

  public static Function<Tuple2<String,String>,String> generateConcatenator(
    Function<Tuple2<String,String>,String> edgeLabelConcatenation) {
    return edgeLabelConcatenation==null ? new StandardStringConcatenation() : edgeLabelConcatenation;
  }

  public static <T> DataSet<T> project(int pos, DataSet<? extends Tuple> dt) {
    return dt.<Tuple1<T>>project(pos).map((Tuple1<T> tt)->tt.f0);
  }

  public static <L,R> JoinOperator<L, R, Tuple2<L, R>> joinAsCross(JoinFunctionAssigner<L,R>
    join, Class<L> lClass, Class<R>
    rClass) {
    return join.with(new JoinFunction<L, R, Tuple2<L,R>>() {
      @Override
      public Tuple2<L, R> join(L first, R second) throws Exception {
        return new Tuple2<L, R>(first,second);
      }
    }).returns(TypeInfoParser.parse(Tuple2.class.getCanonicalName()+"<"+lClass.getCanonicalName()+"," +
      ""+rClass.getCanonicalName()+">"));
  }

  public static <L,R,M> JoinOperator<Tuple2<L, R>, M, Tuple3<L,R,M>> join2AsCross3
    (JoinFunctionAssigner<Tuple2<L, R>, M>
    join, Class<L> lClass, Class<R>
    rClass, Class<M> mClass) {
    return join.with(new JoinFunction<Tuple2<L, R>, M, Tuple3<L,R,M>>() {
      @Override
      public Tuple3<L, R,M> join(Tuple2<L,R> first, M second) throws Exception {
        return new Tuple3<L, R, M>(first.f0,first.f1,second);
      }
    }).returns(TypeInfoParser.parse(Tuple3.class.getCanonicalName()+"<"+lClass.getCanonicalName
      ()+"," +
      ""+rClass.getCanonicalName()+","+mClass.getCanonicalName()+">"));
  }

  //CombiningEdgeTuples

  public static <K extends EPGMElement> Function<Tuple2<K,K>, Boolean> extendBasic
    (Function<K, Function<K, Boolean>> prop) {
    return new Function<Tuple2<K, K>, Boolean>() {

      Function<K, Function<K, Boolean>> local = prop == null ?
        (e1 -> (e2 -> true)) : prop;

      @Override
      public Boolean apply(Tuple2<K, K> entity) {
        final K left = entity.f0;
        final K right = entity.f1;
        HashSet<String> ll = new HashSet<String>();
        left.getProperties().getKeys().forEach(ll::add);
        HashSet<String> rr = new HashSet<String>();
        right.getProperties().getKeys().forEach(rr::add);
        ll.retainAll(rr);
        for (String x : ll) {
          if (!left.getPropertyValue(x).equals(right.getPropertyValue(x)))
            return false;
        }
        return local.apply(left).apply(right);
      }
    };
  }

  public static  Function<Tuple2<CombiningEdgeTuples,CombiningEdgeTuples>, Boolean>
  extendBasic2
    (Function<CombiningEdgeTuples, Function<CombiningEdgeTuples, Boolean>> prop) {
    return new Function<Tuple2<CombiningEdgeTuples,CombiningEdgeTuples>, Boolean>() {
      @Override
      public Boolean apply(final Tuple2<CombiningEdgeTuples,CombiningEdgeTuples> p) {

        Function<CombiningEdgeTuples, Function<CombiningEdgeTuples, Boolean>> local = prop == null ?
          (e1 -> (e2 -> true)) : prop;

            HashSet<String> ll = new HashSet<String>();
            p.f0.f1.getProperties().getKeys().forEach(ll::add);
            HashSet<String> rr = new HashSet<String>();
            p.f1.f1.getProperties().getKeys().forEach(rr::add);
            ll.retainAll(rr);
            for (String x : ll) {
              if (!p.f0.f1.getPropertyValue(x).equals(p.f1.f1.getPropertyValue(x)))
                return false;
            }
            return local.apply(p.f0).apply(p.f1);


      };
    };
  }

}
