package org.gradoop.flink.io.impl.rdbms.sequential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class MigrationOrder {
	ArrayList<String> toNodes;
	ArrayList<RDBMSTable> tables;
	ArrayList<String> toEdges;

	public MigrationOrder(ArrayList<RDBMSTable> tables, ArrayList<String> toEdges) {
		this.toNodes = new ArrayList<String>();
		this.tables = tables;
		this.toEdges = toEdges;
	}

	public ArrayList<String> tablesToNodes() {
		ArrayList<String> candidates = new ArrayList<String>();
		do {
			candidates.clear();
			for (RDBMSTable table : tables) {
				String tableName = table.getTableName();
				if (!toEdges.contains(tableName) && !toNodes.contains(tableName)) {
					if (table.getForeignKeys() == null) {
						candidates.add(tableName);
					} else {
						boolean containsAll = true;
						Iterator it = table.getForeignKeys().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry pair = (Entry) it.next();
							if (!toNodes.contains(pair.getValue())) {
								containsAll = false;
							}
						}
						if (containsAll) {
							candidates.add(tableName);
						}
					}
				}
			}
			for (String c : candidates) {
				toNodes.add(c);
			}
		} while (!candidates.isEmpty());
		return toNodes;
	}
}
