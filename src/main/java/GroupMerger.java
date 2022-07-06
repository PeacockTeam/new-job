import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public record GroupMerger(Map<String, List<String>> m1, Map<String, List<String>> m2) implements Callable<Map<String, List<String>>> {

    @Override
    public Map<String, List<String>> call() {
        Set<String> commonAttributes = new HashSet<>(m2.keySet());
        Set<List<String>> merged = new HashSet<>();
        commonAttributes.retainAll(m1.keySet());
        for (String mapAttribute : m2.keySet()) {
            if (!merged.contains(m2.get(mapAttribute))) {
                if (commonAttributes.contains(mapAttribute)) {
                    merged.add(m2.get(mapAttribute));
                    List<String> group1 = m1.get(mapAttribute);
                    List<String> group2 = m2.get(mapAttribute);
                    Set<String> mergedRecords = new HashSet<>();
                    mergedRecords.addAll(group1);
                    mergedRecords.addAll(group2);
                    List<String> mergedAttributes = mergedRecords.stream()
                            .map(SubgroupAggregator::extractAttributes)
                            .flatMap(Set::stream)
                            .toList();
                    List<String> newGroup = mergedRecords.stream().toList();
                    for (String attribute : mergedAttributes) {
                        m1.put(attribute, newGroup);
                    }
                } else {
                    m1.put(mapAttribute, m2.get(mapAttribute));
                }
            }
        }
        return m1;
    }
}

