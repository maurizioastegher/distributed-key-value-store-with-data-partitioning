package ds;

import java.io.Serializable;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public class NodeList implements Serializable {
    public final int myKey;
    public ConcurrentSkipListMap<Integer, String> addressMap;  //Ordered list of nodes; associates each node's key to its address

    public NodeList(int myKey) {
        this.myKey = myKey;
        addressMap = new ConcurrentSkipListMap<>();
    }

    //Returns a node's address given its key
    public String get(int key) {
        return addressMap.get(key);
    }

    //Adds a new entry to the list of nodes
    public boolean add(int key, String address) {
        addressMap.put(key, address);
        return true;
    }

    //Merges two lists of nodes
    public void merge(NodeList target) {
        addressMap.putAll(target.addressMap);
    }

    //Replaces 'localhost' with the IP address that I learned
    public void refreshMyAddress(String addr) {
        addressMap.keySet().stream().filter(key -> addressMap.get(key).equalsIgnoreCase("//localhost/" + key)).forEach(key -> {
            String newAddress = "//" + addr.split("/")[2] + "/" + key;
            System.out.println("replaced " + addressMap.get(key) + " with " + newAddress);
            addressMap.replace(key, newAddress);
        });
    }

    //Returns the key of the next clockwise neighbour of a given node (providing its key)
    public int getNextNodeKey(int key) {
        if (key == addressMap.lastKey()) {
            return (addressMap.firstKey());
        }
        return addressMap.higherKey(key);
    }

    //Returns the key of the next clockwise neighbour of a given node (providing its address)
    public int getNextNodeKey(String address) {
        return getNextNodeKey(Integer.parseInt(address.split("/")[3]));
    }

    //Returns the address of the next clockwise neighbour of a given node (providing its key)
    public String getNextNode(int key) {
        return addressMap.get(getNextNodeKey(key));
    }

    //Returns the key of the preceding node of a given one (providing its key)
    public int getPreviousNodeKey(int key) {
        if (key == addressMap.firstKey()) {
            return addressMap.lastKey();
        }
        return addressMap.lowerKey(key);
    }

    //Returns the key of the competent node for a given item
    public int getCompetentNodeKey(int key) {
        Integer res;
        res = addressMap.ceilingKey(key);
        if (res == null) {
            res = addressMap.firstKey();
        }
        return res;
    }

    //Returns the address of the competent node for a given item
    public String getCompetentNode(int key) {
        return addressMap.get(getCompetentNodeKey(key));
    }

    //Returns the key of a node which is some position away from me
    public int getNodeKeyByOffset(int offset) {
        return getNodeKeyByOffset(myKey, offset);
    }

    //Returns the key of a node which is some position away from me
    public int getNodeKeyByOffset(int targetKey, int offset) {
        while (offset != 0) {
            if ((offset) > 0) {
                targetKey = getNextNodeKey(targetKey);
                offset--;
            } else {
                targetKey = getPreviousNodeKey(targetKey);
                offset++;
            }
        }
        return targetKey;
    }

    //Returns the address of a node which is some position away from me
    public String getNodeByOffset(int offset) {
        return addressMap.get(getNodeKeyByOffset(offset));
    }

    public String toString() {
        String string = "";
        for (int key : addressMap.keySet()) {
            string = string + "Key: " + key + " address: " + addressMap.get(key) + "\n";
        }
        return string;
    }
}
