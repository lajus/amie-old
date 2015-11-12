package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		ByteString basetype = ByteString.of("<Wikimedia_category_page_Q4167836>");
		amie.data.U.loadSchemaConf();
		KB source = new KB();
		source.load(new File(args[0]));
		Set<ByteString> resultSet = new LinkedHashSet<ByteString>();
		Queue<ByteString> queue = new LinkedList<>();
		Set<ByteString> superTypes = amie.data.U.getSuperTypes(source, basetype);
		queue.addAll(superTypes);
		Map<ByteString, Integer> levels = new HashMap<>();
		int level = 1;
		for (ByteString t : superTypes) {
			levels.put(t, level);
		}
		
		while (!queue.isEmpty()) {
			ByteString currentType = queue.poll();
			System.out.println(currentType + ": " + levels.get(currentType));
			resultSet.add(currentType);
			superTypes = amie.data.U.getSuperTypes(source, currentType);
			
			for (ByteString t : superTypes) {
				if (levels.containsKey(t)) {
					System.out.println(t + ": This type has already a level " + levels.get(t));
					System.out.println("Second level: " + (levels.get(currentType) + 1));
					System.exit(1);
				}
				levels.put(t, levels.get(currentType) + 1);
			}
				
			queue.addAll(superTypes);
		}

	}

}
