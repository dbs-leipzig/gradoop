package org.gradoop.flink.io.impl.rdbms;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class DistinctReduce implements GroupReduceFunction<Tuple4<String,String,String,String>,Tuple2<String,Integer>> {

	@Override
	public void reduce(Iterable<Tuple4<String,String,String,String>> in, Collector<Tuple2<String,Integer>> out) throws Exception {
		// TODO Auto-generated method stub
		String key = null;
		String comp = null;
		
		for(Tuple4<String,String,String,String> t : in){
			key = t.f0;
			
		}
	}

}
