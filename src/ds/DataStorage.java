package ds;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public class DataStorage {
    public ConcurrentSkipListMap<Integer, VersionedValue> map; //Ordered data storage; associates each item's key to its value and version

    public DataStorage() {
        map = new ConcurrentSkipListMap<>();
    }

    public ConcurrentSkipListMap<Integer, VersionedValue> getMap() {
        return map;
    }

    //Returns an item's value given its key
    public VersionedValue get(int key) {
        return map.get(key);
    }

    //Returns the items whose keys fall in a competence interval
    public Map<Integer, VersionedValue> getKeysInRange(int low, int high) {
        ConcurrentSkipListMap<Integer, VersionedValue> result = new ConcurrentSkipListMap<>();
        if (low > high) {
            result.putAll(map.tailMap(low, false));
            result.putAll(map.headMap(high, true));
        } else {
            result.putAll(map.subMap(low, false, high, true));
        }
        return result;
    }

    //Checks if an item's key falls in a competence interval or not
    public static boolean isKeyInRange(int key, int low, int high) {
        if (low > high) {
            return (key > low || key <= high);
        } else {
            return (key > low && key <= high);
        }
    }

    //Adds an item to the data storage
    public boolean add(int key, VersionedValue value) {
        map.put(key, value);
        return true;
    }

    //Adds a list of items to the data storage
    public void addAll(Map<Integer, VersionedValue> newKeys) {
        map.putAll(newKeys);
    }

    //Removes from the data storage the items which are no longer under my competence
    public void trimReplicas(int low, int high) {
        map = (ConcurrentSkipListMap) getKeysInRange(low, high);
    }

    @Override
    public String toString() {
        String string = "";
        for (int key : map.keySet()) {
            VersionedValue versionedValue = map.get(key);
            string = string + "Key: " + key + "\tversion: " + versionedValue.version + "\tvalue: " + versionedValue.value + "\n";
        }
        return string;
    }
}
