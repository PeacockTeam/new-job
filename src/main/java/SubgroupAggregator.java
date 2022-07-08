import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record SubgroupAggregator(Path file, int begin, int end) implements Callable<Map<String, List<String>>> {

    public static Set<String> extractAttributes(String record) {
        return Arrays.stream(record.split(";"))
                .filter(a -> !Objects.equals(a, "\"\""))
                .collect(Collectors.toSet());
    }

    @Override
    public Map<String, List<String>> call() {
        try (Stream<String> lines = Files.lines(file)) {
            Map<String, List<String>> groups = new HashMap<>();
            Iterator<String> lineIterator = lines.skip(begin - 1).iterator();
            for (int i = begin; i <= end; i++) {
                String record = lineIterator.next();
                if (!record.matches(("(\"(0|[1-9][0-9]*)\";|\"\";)+(\"(0|[1-9][0-9]*)\"|\"\")"))) {
                    continue;
                }
                Set<String> attributes = extractAttributes(record);
                Set<String> newRecords = new HashSet<>(){{add(record);}};
                Set<String> newAttributes = new HashSet<>();

                // Case for record with empty strings only
                if (attributes.isEmpty()) {
                    groups.put("emptyGroup"+record.hashCode(), Arrays.asList(record));
                }

                while(!attributes.isEmpty()) {
                    String currentAttribute = attributes.iterator().next();
                    attributes.remove(currentAttribute);
                    newAttributes.add(currentAttribute);
                    if (groups.containsKey(currentAttribute)) {
                        List<String> currentGroup = groups.get(currentAttribute);
                        newRecords.addAll(currentGroup);
                        attributes.addAll(currentGroup.stream()
                                .map(SubgroupAggregator::extractAttributes)
                                .flatMap(Set::stream)
                                .collect(Collectors.toSet())
                                .stream().filter(a -> !newAttributes.contains(a))
                                .toList());
                    }
                }
                List<String> newGroup = newRecords.stream().toList();
                for(String attribute : newAttributes) {
                    groups.put(attribute, newGroup);
                }
            }
            return groups;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
