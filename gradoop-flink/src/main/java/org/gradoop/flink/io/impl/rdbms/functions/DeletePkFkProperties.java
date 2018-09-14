package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Cleans Epgm vertices by deleting primary key and foreign key propeties
 */
public class DeletePkFkProperties extends RichMapFunction<Vertex, Vertex> {

	private static final long serialVersionUID = 1L;
	List<TableToNode> tablesToNodes;

	@Override
	public Vertex map(Vertex v) throws Exception {
		Properties newProps = new Properties();

		for (TableToNode table : tablesToNodes) {
			if (table.getTableName().equals(v.getLabel())) {
				ArrayList<String> foreignKeys = new ArrayList<String>();
				for (RowHeaderTuple rht : table.getRowheader().getForeignKeyHeader()) {
					foreignKeys.add(rht.f0);
				}
				for (Property oldProp : v.getProperties()) {
					if (!foreignKeys.contains(oldProp.getKey()) && !oldProp.getKey().equals(RdbmsConstants.PK_ID)) {
						newProps.set(oldProp);
					}
				}
			}
		}

		v.setProperties(newProps);
		return v;
	}

	public void open(Configuration parameters) throws Exception {
		this.tablesToNodes = getRuntimeContext().getBroadcastVariable("tablesToNodes");
	}
}
