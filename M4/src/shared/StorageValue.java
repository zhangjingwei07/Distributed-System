package shared;
import java.util.*;

public class StorageValue {
    String value = null;
    HashSet<String> subscribers;

    public StorageValue(String _value, HashSet<String> _subscribers) {
        value = _value;
        subscribers = _subscribers;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String _value) {
        value = _value;
    }

    public HashSet<String> getSubscribers() {
        return subscribers;
    }

    public void addSubscriber(String client) {
        subscribers.add(client);
    }

    public void removeSubscriber(String client) {
        subscribers.remove(client);
    }
}
