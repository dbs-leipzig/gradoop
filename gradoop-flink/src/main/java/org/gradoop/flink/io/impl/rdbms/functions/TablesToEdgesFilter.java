package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;


public class TablesToEdgesFilter implements FilterFunction<Tuple2<String,Integer>> {

	@Override
	public boolean filter(Tuple2<String,Integer> in) throws Exception {
		// TODO Auto-generated method stub
		return in.f1 == 2;
	}
}
