package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Tuple representing a gradoop id, key string pair
 */
public class IdKeyTuple extends Tuple2<GradoopId, String> {
	/**
	 * Gradoop id
	 */
	GradoopId id;
	
	/**
	 * Key string
	 */
	String key;

	public IdKeyTuple() {
		
	}
	
	/**
	 * Constructor
	 * @param id Gradoop id
	 * @param key Key string
	 */
	public IdKeyTuple(GradoopId id, String key) {
		this.id = id;
		this.f0 = id;
		this.key = key;
		this.f1 = key;
	}

	public GradoopId getId() {
		return id;
	}

	public void setId(GradoopId id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
