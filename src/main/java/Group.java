import java.util.HashSet;
import java.util.Set;

public class Group {
    private final Set<String> records;
    private final Set<String> attributes;

    Group(Set<String> records, Set<String> attributes) {
        this.records = records;
        this.attributes = attributes;
    }

    Group(String record, Set<String> attributes) {
        this.records = new HashSet<>(){{add(record);}};
        this.attributes = attributes;
    }

    public Set<String> getRecords() {
        return records;
    }

    public Set<String> getAttributes() {
        return attributes;
    }
}
