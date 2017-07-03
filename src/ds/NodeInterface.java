package ds;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public interface NodeInterface extends Remote {
    void get(int key, String clientKey) throws RemoteException;

    void update(int key, String value, String clientKey) throws RemoteException;

    void leave() throws RemoteException;

    String announceJoin(int myKey) throws RemoteException;

    void announceLeave(int key) throws RemoteException;

    VersionedValue read(int updatedKey) throws RemoteException;

    void write(int key, VersionedValue item) throws RemoteException, StorageFullException;

    void writeIfNewer(int key, VersionedValue item) throws RemoteException, StorageFullException;

    void writeAll(Map<Integer, VersionedValue> map) throws RemoteException, StorageFullException;

    Map<Integer, VersionedValue> getKeysInRange(int low, int high) throws RemoteException;

    String listKeys() throws RemoteException;

    boolean isStorageFull() throws RemoteException;

    NodeList getNodeList(String targetAddress) throws RemoteException;

    void updateAllReplicas(String clientKey) throws RemoteException;

    boolean updateNodeReplicas(ConcurrentSkipListSet<String> offlineNodes) throws RemoteException;

    void trimNodeReplicas(Set<String> offlineNodes) throws RemoteException;
}

