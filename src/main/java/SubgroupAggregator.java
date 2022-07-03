import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record SubgroupAggregator(Path file, int begin, int end) implements Callable<Map<String, Group>> {

    @Override
    public Map<String, Group> call() {
        try (Stream<String> lines = Files.lines(file)) {
            Map<String, Group> groups = new HashMap<>();
            Iterator<String> lineIterator = lines.skip(begin - 1).iterator();
            for (int i = begin; i <= end; i++) {
                String record = lineIterator.next();
                if (!record.matches(("(\"(0|[1-9][0-9]*)\";|\"\";)+(\"(0|[1-9][0-9]*)\"|\"\")*"))) {
                    continue;
                }
                Set<String> attributes = Arrays.stream(record.split(";"))
                        .filter(a -> !Objects.equals(a, "\"\""))
                        .collect(Collectors.toSet());
                List<Group> groupsToMerge = new ArrayList<>();
                for (String attribute : attributes) {
                    if (groups.containsKey(attribute)) {
                        groupsToMerge.add(groups.get(attribute));
                    }
                }
                if (groupsToMerge.isEmpty()) {
                    Group group = new Group(record, attributes);
                    for (String attribute : attributes) {
                        groups.put(attribute, group);
                    }
                } else {
                    Set<String> groupRecords = new HashSet<>();
                    Set<String> groupAttributes = new HashSet<>();
                    for (Group g : groupsToMerge) {
                        groupRecords.addAll(g.getRecords());
                        groupAttributes.addAll(g.getAttributes());
                    }
                    groupRecords.add(record);
                    groupAttributes.addAll(attributes);
                    Group group = new Group(groupRecords, groupAttributes);
                    for (String a : groupAttributes) {
                        groups.put(a, group);
                    }
                }
            }
            return groups;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
