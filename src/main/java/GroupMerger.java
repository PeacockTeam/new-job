import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public record GroupMerger(Map<String, Group> m1, Map<String, Group> m2) implements Callable<Map<String, Group>> {

    @Override
    public Map<String, Group> call() {
        Set<String> commonAttributes = new HashSet<>(m2.keySet());
        Set<Group> merged = new HashSet<>();
        commonAttributes.retainAll(m1.keySet());
        for (String mapAttribute : m2.keySet()) {
            if (!merged.contains(m2.get(mapAttribute))) {
                if (commonAttributes.contains(mapAttribute)) {
                    merged.add(m2.get(mapAttribute));
                    Group group1 = m1.get(mapAttribute);
                    Group group2 = m2.get(mapAttribute);
                    group1.getRecords().addAll(group2.getRecords());
                    group1.getAttributes().addAll(group2.getAttributes());
                    for (String attribute : group2.getAttributes()) {
                        m1.put(attribute, group1);
                    }
                } else {
                    m1.put(mapAttribute, m2.get(mapAttribute));
                }
            }
        }
        return m1;
    }
}

