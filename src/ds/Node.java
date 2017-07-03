package ds;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.rmi.Naming.lookup;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public class Node extends UnicastRemoteObject implements NodeInterface {
    public static final int M = 20;     //Max nodes
    public static final int MK = 65535; //Max key value
    public static final int I = 200;    //Max keys per node
    public static final int N = 3;      //Number of replicas
    public static final int R = 2;      //Read quorum
    public static final int W = 2;      //Write quorum (R + W > N)
    public static final int T = 2000;   //Timeout

    public final int myKey;     //The node's key
    public String myAddress;    //The node's address (in the format //ipAddress/key)

    private NodeList nodeList;          //List of nodes in the system (that I know of)
    private DataStorage dataStorage;    //Where to keep all the items
    private final String storeFile;     //File on disk for recovering the dataStorage in case of failure

    private static final boolean optionalDelays = false;

    public Node(int myKey, String contactAddress, String loadFile) throws RemoteException {
        super(0);
        this.myKey = myKey;

        //Check if the parameters are valid
        if (myKey > MK || myKey < 0) {
            System.out.println("Node key out of bounds");
            System.exit(1);
        }
        if (R + W <= N) {
            System.out.println("Quorum parameters not valid: R(" + R + ") + W(" + W + ") <= N(" + N + ")");
            System.exit(1);
        }

        //Register the node on the RMI registry
        Registry registry = LocateRegistry.getRegistry();
        registry.rebind("" + myKey, this);
        System.out.println("Node " + myKey + " bound in registry\n");

        //Initialize my nodeList (containing only myself at the moment) and dataStorage
        nodeList = new NodeList(myKey);
        nodeList.add(myKey, "//localhost/" + myKey);
        dataStorage = new DataStorage();

        //Check if the local storeFile exists. In that case perform the recovery, otherwise the join procedure
        storeFile = myKey + ".txt";
        if (new File(storeFile).exists()) {
            System.out.println("Found local key store, recovering...");
            recovery(contactAddress);
        } else {
            try {
                new File(storeFile).createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (!contactAddress.equalsIgnoreCase("localhost")) {
                join(contactAddress);
            }
        }

        //If a loadFile is specified, load values from the file and treat them as update requests.
        if (loadFile != null) {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(loadFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    String[] split = line.split("\t");
                    update(Integer.parseInt(split[0]), split[1], "");
                }
            } catch (IOException | NumberFormatException e) {
                System.out.println("File " + loadFile + " not found or not valid");
            }
        }
    }

    public static void main(String args[]) throws Exception {
        //Instantiate the node
        if (args.length < 2 || args.length > 3) {
            System.out.println(
                    "\nThis command instantiates a node with the specified key.\n" +
                            "Please, specify the address of an existing node or \"localhost\" if this is the first node of a new distributed store.\n" +
                            "Usage:\t\tjava ds.Node node_key address_to_connect_to [file_with_items_to_add_automatically]\n" +
                            "Examples:\tjava ds.Node 3 //localhost/1\n" +
                            "\t\tjava ds.Node 3 localhost\n" +
                            "\t\tjava ds.Node 5 //localhost/3 load.txt\n");
            return;
        }

        //Connect to the RMI registry
        System.out.println("Node started");
        try {
            LocateRegistry.createRegistry(1099);
            System.out.println("********* WARNING: please, run RegistryInitializer on every host first." +
                    " Local registry created, but bound to the lifecycle of this process\n");
        } catch (RemoteException e) {
            System.out.println("RMI registry found");
        }

        String fileName = null;
        if (args.length == 3) {
            fileName = args[2];
        }

        try {
            new Node(Integer.parseInt(args[0]), args[1], fileName);
        } catch (NumberFormatException e) {
            System.err.println("Node key must be an integer");
            return;
        }
    }

    //Inserts this node into the network
    public void join(String contactAddress) {
        ConcurrentSkipListSet<String> offlineNodes = new ConcurrentSkipListSet<>();
        try {
            try {
                //Request from contactAddress the whole list of participants; exit if M is reached
                NodeInterface contact = (NodeInterface) lookup(contactAddress);
                NodeList contactedNodeList = contact.getNodeList(contactAddress);
                nodeList.merge(contactedNodeList);
                if (nodeList.addressMap.size() > M) {
                    System.out.println("Maximum number of nodes reached, join aborted");
                    System.exit(1);
                }
            } catch (RemoteException e) {
                System.out.println("Contact node not responding, join aborted");
                System.exit(1);
            }

            //Before joining, try to update replicas of next clockwise neighbour and the N - 1 preceding nodes
            //(this avoids getting outdated information)
            String nextAddress = nodeList.getNextNode(myKey);
            Set<String> nodesToUpdate = new HashSet<>();
            nodesToUpdate.add(nextAddress);
            for (int i = 1; i <= N; i++) {
                nodesToUpdate.add(nodeList.getNodeByOffset(i * (-1)));
            }
            System.out.println("Updating neighbours before joining...");
            boolean updateResult = updateReplicas(nodesToUpdate, offlineNodes);
            if (!updateResult) {
                System.out.println("********* WARNING: could not update all values\n");
            }

            //Announce my presence to everybody
            AtomicBoolean myAddressRefreshed = new AtomicBoolean(false);
            Set<Thread> announceThreads = new HashSet<>();
            for (int key : nodeList.addressMap.keySet()) {
                if (key != myKey) {
                    AnnounceJoinThread t = new AnnounceJoinThread(offlineNodes, myAddressRefreshed, key);
                    t.start();
                    announceThreads.add(t);
                }
            }
            for (Thread t : announceThreads) {
                try {
                    t.join(T);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Announced my presence");
            System.out.println("Retrieving keys...");
            //Request data that fall in my range from next clockwise neighbour
            try {
                if (!offlineNodes.contains(nextAddress)) {
                    System.out.println("The next clockwise neighbour is online (" + nextAddress + ")");
                    try {
                        NodeInterface node = (NodeInterface) lookup(nextAddress);
                        Map<Integer, VersionedValue> newKeys = node.getKeysInRange(nodeList.getNodeKeyByOffset((N) * (-1)), myKey);
                        writeAll(newKeys);
                    } catch (RemoteException e) {
                        //Should never happen
                    }
                } else {
                    //The next clockwise neighbour is offline; try to contact someone else going forward (max N - 1 steps)
                    System.out.println("The next clockwise neighbour is offline (" + nextAddress + ")\nGoing forward...");
                    for (int i = 2; i <= N; i++) {
                        String nodeAddress = nodeList.getNodeByOffset(i);
                        if (!offlineNodes.contains(nodeAddress)) {
                            try {
                                System.out.println("contacting node: " + nodeAddress);
                                NodeInterface node = (NodeInterface) lookup(nodeAddress);
                                Map<Integer, VersionedValue> newKeys = node.getKeysInRange(nodeList.getNodeKeyByOffset((N) * (-1)), myKey);
                                writeAll(newKeys);
                                break;
                            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    //The next clockwise neighbour is offline; try to contact someone else going backward (max N - 1 steps)
                    System.out.println("The next clockwise neighbour is offline (" + nextAddress + ")\nGoing backward...");
                    for (int i = 1; i < N; i++) {
                        String nodeAddress = nodeList.getNodeByOffset(i * (-1));
                        if (!offlineNodes.contains(nodeAddress)) {
                            try {
                                System.out.println("contacting node: " + nodeAddress);
                                NodeInterface node = (NodeInterface) lookup(nodeAddress);
                                Map<Integer, VersionedValue> newKeys = node.getKeysInRange(nodeList.getNodeKeyByOffset((N) * (-1)), myKey);
                                writeAll(newKeys);
                                break;
                            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } catch (StorageFullException e) {
                System.out.println("********* WARNING: the local storage is full\n");
            }

            //Tell old replicas to discard keys no longer of their competence
            Set<String> nodesToTrim = new HashSet<>();
            for (int i = 1; i <= N; i++) {
                String nodeAddress = nodeList.getNodeByOffset(i);
                if (!offlineNodes.contains(nodeAddress) && !nodeAddress.equalsIgnoreCase(myAddress)) {
                    nodesToTrim.add(nodeAddress);
                }
            }
            trimReplicas(nodesToTrim, offlineNodes);
            System.out.println("Join complete\n");

        } catch (NotBoundException e) {
            System.out.println(contactAddress + " not in the registry, join aborted");
            System.exit(1);
        } catch (MalformedURLException e) {
            System.out.println("Malformed address, join aborted");
            System.exit(1);
        }
    }

    @Override
    //Adds the new node to the nodeList and returns its address
    public String announceJoin(int key) {
        try {
            String client = getClientHost();
            if (getClientHost().equalsIgnoreCase(InetAddress.getLocalHost().getHostAddress()))
                client = "//localhost/" + key;
            else
                client = "//" + client + "/" + key;
            nodeList.add(key, client);
            //System.out.println("After receiving announceJoin from " + key + ":\n" + nodeList);
            return client;
        } catch (ServerNotActiveException | UnknownHostException e) {
            e.printStackTrace(); //Should never happen
        }
        return null; //Should never happen
    }

    @Override
    public void get(int getKey, String clientKey) throws RemoteException {
        //We are the coordinator of the GET request
        String client;
        try {
            client = "//" + getClientHost() + "/" + clientKey;
            System.out.println("GET request from client " + client);
            new GetThread(getKey, client).start(); //Start a new thread to handle the request
        } catch (ServerNotActiveException e) {
            e.printStackTrace(); //Should never happen
        }
    }

    @Override
    public void update(int updateKey, String value, String clientKey) throws RemoteException {
        //We are the coordinator of the UPDATE request
        String client;
        try {
            client = "//" + getClientHost() + "/" + clientKey;
            System.out.println("UPDATE request from client " + client);
            //System.out.println(nodeList.toString());
        } catch (ServerNotActiveException e) {
            client = "";
        }
        new UpdateThread(updateKey, client, value).start(); //Start a new thread to handle the request
    }

    @Override
    public void leave() throws RemoteException {
        //Announce everybody that I'm leaving
        Set<Thread> announceThreads = new HashSet<>();
        for (int key : nodeList.addressMap.keySet()) {
            if (key != myKey) {
                Thread t = new AnnounceLeaveThread(key);
                announceThreads.add(t);
                t.start();
            }
        }
        for (Thread t : announceThreads) {
            try {
                t.join(T);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Pass each key to the new replica
        Set<Thread> threads = new HashSet<>();
        int low = myKey;
        for (int k = 1; k <= N; k++) {
            int high = low;
            low = nodeList.getPreviousNodeKey(low);
            Map<Integer, VersionedValue> valuesInRange = dataStorage.getKeysInRange(low, high);
            int targetNodeKey = nodeList.getNodeKeyByOffset(N - (k - 1));
            Thread t = new SendMapThread(nodeList.get(targetNodeKey), valuesInRange);
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Unbind the node from the registry
        System.out.println("I'm leaving, bye!");
        Registry registry = LocateRegistry.getRegistry();
        try {
            registry.unbind("" + myKey);
        } catch (NotBoundException e) {
            e.printStackTrace();
        }

        //Delete the storage file from disk
        new File(storeFile).delete();

        //Wait for the method to return before terminating the process
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        }.start();
    }

    @Override
    //Removes the node from the list of currently running nodes
    public void announceLeave(int key) {
        nodeList.addressMap.remove(key);
    }

    //Called when the current node is restarted after a crash.
    private void recovery(String contactAddress) {
        ConcurrentSkipListSet<String> offlineNodes = new ConcurrentSkipListSet<>();

        //If I'm the only node in the system...
        if (contactAddress.equalsIgnoreCase("localhost")) {
            //Just read the items from the file and put them in the data storage.
            new File(storeFile).renameTo(new File("temp" + myKey + ".txt"));
            try {
                new File(storeFile).createNewFile();
                BufferedReader reader = Files.newBufferedReader(Paths.get("temp" + myKey + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    String[] split = line.split("\t");
                    int key = Integer.parseInt(split[0]);
                    VersionedValue value = new VersionedValue(split[1], Integer.parseInt(split[2]));
                    write(key, value);
                }
                new File("temp" + myKey + ".txt").delete();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (StorageFullException e) {
                System.out.println("Storage full");
            }
        } else { //If I'm not the only node in the system...
            try {
                System.out.println("Retrieving the node list...");
                NodeInterface contact = (NodeInterface) lookup(contactAddress);
                NodeList contactedNodeList = contact.getNodeList(contactAddress); //Request the current list of nodes
                nodeList.merge(contactedNodeList);
                myAddress = nodeList.get(myKey);
                int lowKey = nodeList.getNodeKeyByOffset(N * (-1));

                //Read the list of items from the file
                System.out.println("Restoring items from file...");
                new File(storeFile).renameTo(new File("temp" + myKey + ".txt"));
                new File(storeFile).createNewFile();
                try (BufferedReader reader = Files.newBufferedReader(Paths.get("temp" + myKey + ".txt"))) {
                    String line;
                    Map<String, Map<Integer, VersionedValue>> toSend = new ConcurrentSkipListMap<>();
                    while ((line = reader.readLine()) != null) { //For each item...
                        if (line.trim().isEmpty()) {
                            continue;
                        }
                        String[] split = line.split("\t");
                        int key = Integer.parseInt(split[0]);
                        VersionedValue value = new VersionedValue(split[1], Integer.parseInt(split[2]));
                        //Check if it's still under my competence...
                        if (DataStorage.isKeyInRange(key, lowKey, myKey)) {
                            //If so, write it to the local storage...
                            write(key, value);
                        } else {
                            //Otherwise, put it in a list (it will be sent to the competent node)
                            String competentNode = nodeList.getCompetentNode(key);
                            if (toSend.get(competentNode) == null) {
                                Map<Integer, VersionedValue> map = new HashMap<>();
                                map.put(key, value);
                                toSend.put(competentNode, map);
                            } else {
                                toSend.get(competentNode).put(key, value);
                            }
                        }

                    }
                    //Send the items to the competent nodes, in parallel
                    Set<Thread> threads = new HashSet<>();
                    for (String nodeAddress : toSend.keySet()) {
                        Thread t = new SendMapThread(nodeAddress, toSend.get(nodeAddress));
                        t.start();
                        threads.add(t);
                    }
                    for (Thread t : threads) {
                        try {
                            t.join(T);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (StorageFullException ignored) {
                }
                new File("temp" + myKey + ".txt").delete();

                //Update my replicas and the ones of my N - 1 preceding nodes
                Set<String> nodesToUpdate = new HashSet<>();
                for (int i = 0; i < N; i++) {
                    nodesToUpdate.add(nodeList.getNodeByOffset(i * (-1)));
                }
                System.out.println("Updating neighbours...");
                updateReplicas(nodesToUpdate, offlineNodes);

            } catch (RemoteException | NotBoundException e) {
                System.out.println("Contact node not responding, recovery aborted.");
                System.exit(1);
            } catch (IOException e) {
                System.out.println("File not found, recovery aborted.");
                System.exit(1);
            } catch (NumberFormatException e) {
                System.out.println("File not valid, recovery aborted.");
                System.exit(1);
            }
        }
        System.out.println("Recovery completed\n");
    }

    @Override
    //Returns the list of nodes currently running and refreshes my address if needed
    public NodeList getNodeList(String address) {
        //System.out.println("getNodeList(" + address + ")");
        if (!"localhost".equalsIgnoreCase(address.split("/")[2]) && !address.equalsIgnoreCase(myAddress)) {
            nodeList.refreshMyAddress(address);
        }
        return nodeList;
    }

    @Override
    //Returns the items that fall in the specified range
    public Map<Integer, VersionedValue> getKeysInRange(int low, int high) {
        return dataStorage.getKeysInRange(low, high);
    }

    //Calls trimNodeReplicas on the specified nodes, in parallel
    public void trimReplicas(Set<String> nodes, Set<String> offlineNodes) {
        Set<Thread> threads = new HashSet<>();
        for (String address : nodes) {
            if (offlineNodes.contains(address)) continue;
            //System.out.println("Trimming replicas of " + address);
            Thread t = new TrimReplicasThread(address, offlineNodes);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join(T);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    //Checks which keys are no longer under my competence and passes them to competent nodes
    public void trimNodeReplicas(Set<String> offlineNodes) throws RemoteException {
        ConcurrentSkipListMap<Integer, VersionedValue> diff = new ConcurrentSkipListMap<>();
        diff.putAll(dataStorage.getMap());
        dataStorage.trimReplicas(nodeList.getNodeKeyByOffset((N) * (-1)), myKey);
        diff.entrySet().removeAll(dataStorage.getMap().entrySet());
        saveKeys(); //Update the local file

        Map<String, Map<Integer, VersionedValue>> toSend = new ConcurrentSkipListMap<>();

        //Prepare a list of items for each competent node
        for (int key : diff.keySet()) {
            String competentNode = nodeList.getCompetentNode(key);
            if (toSend.get(competentNode) == null) {
                Map<Integer, VersionedValue> map = new HashMap<>();
                map.put(key, diff.get(key));
                toSend.put(competentNode, map);
            } else {
                toSend.get(competentNode).put(key, diff.get(key));
            }
        }

        //Send each list of items, in parallel
        Set<Thread> threads = new HashSet<>();
        for (String nodeAddress : toSend.keySet()) {
            Thread t = new SendMapThread(nodeAddress, toSend.get(nodeAddress));
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            try {
                t.join(T);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    //Simply returns all the items in the local storage
    public String listKeys() {
        return dataStorage.toString();
    }

    @Override
    //Simply returns a single item from the local storage
    public VersionedValue read(int key) throws RemoteException {
        if (optionalDelays) {
            try {
                int time = (int) (Math.random() * T * 2);
                System.out.println("Delaying node reply for " + time + "ms");
                Thread.sleep(time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return dataStorage.get(key);
    }

    @Override
    //Writes a single item to the storage and updates the file
    public void write(int key, VersionedValue item) throws RemoteException, StorageFullException {
        write(key, item, true);
    }

    @Override
    //Writes a single item in the storage and updates the file, if it's newer
    public void writeIfNewer(int key, VersionedValue item) throws RemoteException, StorageFullException {
        writeIfNewer(key, item, true);
    }

    @Override
    //Writes a list of items in the storage and updates the file
    public void writeAll(Map<Integer, VersionedValue> map) throws RemoteException, StorageFullException {
        for (int key : map.keySet()) {
            writeIfNewer(key, map.get(key), false);
        }
        saveKeys();
    }

    //Writes a single item in the local storage and updates the file if requested
    private void write(int key, VersionedValue item, boolean saveToFile) throws RemoteException, StorageFullException {
        if (dataStorage.get(key) == null && isStorageFull()) {
            throw new StorageFullException();
        } else {
            dataStorage.add(key, item);
            if (saveToFile) {
                saveKeys();
            }
        }
    }

    //Writes a single value in the storage, if it's newer, and updates the file if requested
    private void writeIfNewer(int key, VersionedValue item, boolean saveToFile) throws RemoteException, StorageFullException {
        VersionedValue oldItem = dataStorage.get(key);
        if (oldItem == null || oldItem.version < item.version) {
            write(key, item, saveToFile);
        }
    }

    //Writes all the items in the storage to file
    private void saveKeys() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(storeFile))) {
            for (int k : dataStorage.map.keySet()) {
                writer.write(k + "\t" + dataStorage.get(k).value + "\t" + dataStorage.get(k).version);
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    //Updates all the items in the system to the latest version
    public void updateAllReplicas(String clientKey) {
        String client = "";
        try {
            client = "//" + getClientHost() + "/" + clientKey;
        } catch (ServerNotActiveException ignored) {
        }
        HashSet<String> valuesSet = new HashSet<>(nodeList.addressMap.values());
        boolean result = updateReplicas(valuesSet, new ConcurrentSkipListSet<String>());
        returnResults(result ? "OK" : "FAIL", client);
    }

    //Calls updateNodeReplicas on each specified node, in parallel
    public boolean updateReplicas(Set<String> nodes, ConcurrentSkipListSet<String> offlineNodes) {
        AtomicBoolean result = new AtomicBoolean(true);
        Set<Thread> threads = new HashSet<>();
        for (String address : nodes) {
            if (offlineNodes.contains(address)) continue;
            //System.out.println("Updating replicas of " + address);
            Thread t = new UpdateReplicasThread(address, result, offlineNodes);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join(T);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return result.get();
    }

    @Override
    //Updates all the items that fall in my range, comparing them with the ones of my replicas;
    //out-of-date replicas are then updated
    public boolean updateNodeReplicas(ConcurrentSkipListSet<String> offlineNodes) {
        boolean result;
        Map<String, Map<Integer, VersionedValue>> replicasValues = new ConcurrentSkipListMap<>();
        int previousKey = nodeList.getPreviousNodeKey(myKey);
        AtomicInteger replies = new AtomicInteger(0);
        if (offlineNodes == null) {
            offlineNodes = new ConcurrentSkipListSet<>();
        }
        CountDownLatch latch = new CountDownLatch(N - 1);

        //Request items to my replicas, in parallel
        for (int i = 1; i < N; i++) {
            String nodeAddress = nodeList.getNodeByOffset(i);
            if (offlineNodes.contains(nodeAddress)) continue;
            new Thread(new UpdateNodeReplicasRunnable(nodeAddress, latch, offlineNodes, replicasValues, previousKey, replies))
                    .start();
        }

        try {
            latch.await(T, TimeUnit.MILLISECONDS);  //Wait for my replicas to reply or the timeout to expire
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Continue only if at least max(R, W) replicas replied (including me)
        if (replies.get() >= (Math.max(R, W) - 1)) {
            Map<Integer, VersionedValue> complete = new ConcurrentSkipListMap<>();
            for (int key : dataStorage.map.keySet()) { //Prepare the items in my storage for distribution
                if (DataStorage.isKeyInRange(key, previousKey, myKey)) {
                    complete.put(key, dataStorage.get(key));
                }
            }

            //Find the latest version of each item
            for (String nodeAddress : replicasValues.keySet()) {
                for (int key : replicasValues.get(nodeAddress).keySet()) {
                    VersionedValue newValue = replicasValues.get(nodeAddress).get(key);
                    VersionedValue oldValue = complete.get(key);
                    if (oldValue == null || oldValue.version < newValue.version) {
                        complete.put(key, newValue);
                    }
                }
            }

            try {
                writeAll(complete); //Save the updated items in my local storage
            } catch (RemoteException e) {
                e.printStackTrace(); //local call, a RemoteException should never happen
            } catch (StorageFullException e) {
                System.out.println();
            }

            //Check if the nodes have outdated items and in that case send the latest versions to them, in parallel
            Set<Thread> threads = new HashSet<>();
            for (String nodeAddress : replicasValues.keySet()) {
                if (!offlineNodes.contains(nodeAddress)) {
                    Thread t = new UpdateNodeReplicasWriteThread(nodeAddress, complete, replicasValues);
                    t.start();
                    threads.add(t);
                }
            }
            for (Thread t : threads) {
                try {
                    t.join(T);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            result = true;
        } else {
            System.out.println("updateNodeReplicas: not enough replies(" + replies + ")");
            result = false;
        }
        return result;
    }

    @Override
    //Checks if my local storage is full
    public boolean isStorageFull() throws RemoteException {
        return (dataStorage.getMap().size() >= I);
    }

    //Returns the results of an operation to the client that requested it
    private void returnResults(String result, String address) {
        if (address == null || address.isEmpty()) return;
        try {
            System.out.println("Returning result \"" + result + "\" to client " + address + "\n");
            ClientInterface client = (ClientInterface) lookup(address);
            client.returnResults(result);
        } catch (NotBoundException | MalformedURLException | RemoteException e) {
            e.printStackTrace();
        }
    }

    //Thread that performs a single GET operation
    private class GetThread extends Thread {
        private int getKey;
        private String client;

        public GetThread(int getKey, String client) {
            this.getKey = getKey;
            this.client = client;
        }

        @Override
        public void run() {
            if (getKey > MK || getKey < 0) {
                returnResults("Key out of bounds", client);
                return;
            }
            ArrayList<String> contactedNodes = new ArrayList<>();
            for (int i = 0; i < N; i++) { //Find the competent node and its replicas
                if (i == 0) {
                    contactedNodes.add(nodeList.getCompetentNode(getKey));
                } else {
                    contactedNodes.add(nodeList.get(nodeList.getNextNodeKey(contactedNodes.get(i - 1))));
                }
            }

            ConcurrentSkipListMap<String, VersionedValue> replicasValues = new ConcurrentSkipListMap<>();
            AtomicInteger replies = new AtomicInteger(0);
            CountDownLatch readLatch = new CountDownLatch(R);

            //Ask each replica to return the requested item, in parallel
            for (String nodeAddress : contactedNodes) {
                new Thread(new GetRunnable(nodeAddress, replicasValues, replies, getKey, readLatch)).start();
            }
            try { //Wait for at least R replicas to reply or the timeout to expire
                readLatch.await(T, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (replies.get() < R) {
                returnResults("FAIL", client);
                return;
            }

            //Find the latest version of the item
            int maxVersion = 0;
            String maxVersionValue = null;
            for (String key : replicasValues.keySet()) {
                VersionedValue versionedValue = replicasValues.get(key);
                if (versionedValue != null) {
                    if (versionedValue.version > maxVersion) {
                        maxVersion = versionedValue.version;
                        maxVersionValue = versionedValue.value;
                    }
                }
            }
            returnResults(maxVersionValue, client); //Return the value to the client
        }
    }

    //Read the requested item from the specified node
    private class GetRunnable implements Runnable {
        String nodeAddress;
        ConcurrentSkipListMap<String, VersionedValue> replicasValues;
        AtomicInteger replies;
        int getKey;
        CountDownLatch latch;

        public GetRunnable(String nodeAddress, ConcurrentSkipListMap<String, VersionedValue> replicasValues,
                           AtomicInteger replies, int getKey, CountDownLatch latch) {
            this.nodeAddress = nodeAddress;
            this.replicasValues = replicasValues;
            this.replies = replies;
            this.getKey = getKey;
            this.latch = latch;
        }

        @Override
        public void run() {
            //System.out.println("GET contacting node: " + nodeAddress);
            NodeInterface node = null;
            try {
                node = (NodeInterface) lookup(nodeAddress);
            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                e.printStackTrace();
            }
            assert node != null;

            try {
                VersionedValue returnedValue = node.read(getKey); //Read the item from the local storage
                if (returnedValue != null) {
                    replicasValues.put(nodeAddress, returnedValue); //Put the item in the map
                }
                replies.getAndIncrement();
            } catch (RemoteException e) {
                System.out.println("Node " + nodeAddress + " offline");
            }
            latch.countDown(); //Signal the end of the operation
        }
    }

    //Thread that performs a single UPDATE operation
    private class UpdateThread extends Thread {
        String client;
        String value;
        private int updateKey;

        public UpdateThread(int updateKey, String client, String value) {
            this.updateKey = updateKey;
            this.client = client;
            this.value = value;
        }

        @Override
        public void run() {
            if (updateKey > MK || updateKey < 0) {
                returnResults("Key out of bounds", client);
                return;
            }
            Set<String> offlineNodes = new ConcurrentSkipListSet<>();
            ArrayList<String> contactedNodes = new ArrayList<>();
            for (int i = 0; i < N; i++) { //Find the competent node and its replicas
                if (i == 0) {
                    contactedNodes.add(nodeList.getCompetentNode(updateKey));
                } else {
                    contactedNodes.add(nodeList.get(nodeList.getNextNodeKey(contactedNodes.get(i - 1))));
                }
            }

            ConcurrentSkipListMap<String, VersionedValue> replicasValues = new ConcurrentSkipListMap<>();
            AtomicInteger replies = new AtomicInteger(0);
            AtomicInteger nodesNotFull = new AtomicInteger(0);
            CountDownLatch repliesLatch = new CountDownLatch(Math.max(R, W));
            CountDownLatch notFullLatch = new CountDownLatch(W);
            //Ask each replica to return the requested item, in parallel
            for (String nodeAddress : contactedNodes) {
                new Thread(new UpdateReadRunnable(nodeAddress, replicasValues, replies, updateKey, repliesLatch,
                        notFullLatch, offlineNodes, nodesNotFull)).start();
            }

            try {
                long startWaiting = System.currentTimeMillis();
                //Wait for at least max(R, W) replicas to reply or the timeout to expire
                repliesLatch.await(T, TimeUnit.MILLISECONDS);
                long timeWaited = System.currentTimeMillis() - startWaiting;
                long remainingTime = T - timeWaited;
                if (remainingTime < 0) remainingTime = 0;
                //If less then W of the nodes that replied were not full (and we know that with the nodes that have not
                //replied yet we can reach the write quorum), keep waiting.
                if (nodesNotFull.get() < W && replies.get() >= Math.max(R, W) && W - nodesNotFull.get() <= N - replies.get()) {
                    System.out.println("Waiting for W nodes not full");
                    notFullLatch.await(remainingTime, TimeUnit.MILLISECONDS);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (replies.get() < Math.max(R, W)) {
                returnResults(replies + " replies: max(R, W) not reached", client);
                return;
            }
            if (nodesNotFull.get() < W) {
                returnResults(nodesNotFull + " nodes not full: W not reached", client);
                return;
            }

            //Find the latest version of the item
            int maxVersion = 0;
            for (String key : replicasValues.keySet()) {
                VersionedValue versionedValue = replicasValues.get(key);
                if (versionedValue != null) {
                    maxVersion = Math.max(maxVersion, versionedValue.version);
                }
            }

            //Create the new item
            VersionedValue newItem = new VersionedValue(value, maxVersion + 1);
            CountDownLatch writeLatch = new CountDownLatch(W);

            //Ask each replica to write the new item, in parallel
            for (String nodeAddress : contactedNodes) {
                if (offlineNodes.contains(nodeAddress)) continue; //Skip known offline or full full nodes
                new Thread(new UpdateWriteRunnable(nodeAddress, updateKey, writeLatch, offlineNodes, newItem)).start();
            }
            try { //Wait for at least W replicas to complete or the timeout to expire
                writeLatch.await(T, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            returnResults("OK", client);
        }
    }

    //Read the requested item from the specified node
    private class UpdateReadRunnable implements Runnable {
        String nodeAddress;
        ConcurrentSkipListMap<String, VersionedValue> replicasValues;
        AtomicInteger replies;
        AtomicInteger nodesNotFull;
        int updateKey;
        CountDownLatch repliesLatch;
        Set<String> offlineNodes;
        CountDownLatch notFullLatch;

        public UpdateReadRunnable(String nodeAddress, ConcurrentSkipListMap<String, VersionedValue> replicasValues,
                                  AtomicInteger replies, int updateKey, CountDownLatch repliesLatch,
                                  CountDownLatch notFullLatch, Set<String> offlineNodes, AtomicInteger nodesNotFull) {
            this.nodeAddress = nodeAddress;
            this.replicasValues = replicasValues;
            this.replies = replies;
            this.updateKey = updateKey;
            this.repliesLatch = repliesLatch;
            this.notFullLatch = notFullLatch;
            this.nodesNotFull = nodesNotFull;
            this.offlineNodes = offlineNodes;
        }

        @Override
        public void run() {
            NodeInterface node = null;
            try {
                node = (NodeInterface) lookup(nodeAddress);
            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                e.printStackTrace();
            }
            assert node != null;
            try {
                VersionedValue returnedValue = node.read(updateKey); //Read the item from the local storage
                if (returnedValue != null) {
                    replicasValues.put(nodeAddress, returnedValue); //Put the item in the map
                    nodesNotFull.getAndIncrement(); //Increment the number of nodes that can write the new value
                    notFullLatch.countDown();
                } else {
                    //Item not present; check if the node's storage is full
                    if (!node.isStorageFull()) {
                        //The storage is not full; increment the number of nodes that can write the new value
                        nodesNotFull.getAndIncrement();
                        notFullLatch.countDown();
                    } else {
                        System.out.println(nodeAddress + " has storage full");
                        offlineNodes.add(nodeAddress); //Since this node is full, I'm not going to write on it
                    }
                }
                replies.getAndIncrement(); //Increment the number of replies
            } catch (RemoteException e) {
                offlineNodes.add(nodeAddress); //Node not responding; add it to the list of offline nodes
            }
            repliesLatch.countDown(); //Signal the end of the operation
        }
    }

    //Writes the new item on the specified node
    private class UpdateWriteRunnable implements Runnable {
        String nodeAddress;
        int updateKey;
        CountDownLatch latch;
        Set<String> offlineNodes;
        VersionedValue newItem;

        public UpdateWriteRunnable(String nodeAddress, int updateKey, CountDownLatch latch, Set<String> offlineNodes,
                                   VersionedValue newItem) {
            this.nodeAddress = nodeAddress;
            this.updateKey = updateKey;
            this.latch = latch;
            this.offlineNodes = offlineNodes;
            this.newItem = newItem;
        }

        @Override
        public void run() {
            NodeInterface node = null;
            try {
                node = (NodeInterface) lookup(nodeAddress);
            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                e.printStackTrace();
            }
            assert node != null;
            try {
                node.write(updateKey, newItem);
            } catch (StorageFullException e) {
                System.out.println("********* WARNING: the local storage is full (" + nodeAddress + ")");
            } catch (RemoteException ignored) {
            }
            latch.countDown();
        }
    }

    //Calls announceJoin on the specified node
    private class AnnounceJoinThread extends Thread {
        Set<String> offlineNodes;
        AtomicBoolean myAddressRefreshed;
        int key;

        public AnnounceJoinThread(Set<String> offlineNodes, AtomicBoolean myAddressRefreshed, int key) {
            this.offlineNodes = offlineNodes;
            this.myAddressRefreshed = myAddressRefreshed;
            this.key = key;
        }

        @Override
        public void run() {
            String address = nodeList.addressMap.get(key);
            if (offlineNodes.contains(address)) return;
            try {
                NodeInterface node = (NodeInterface) lookup(address);
                myAddress = node.announceJoin(myKey);
                if (!"localhost".equalsIgnoreCase(myAddress.split("/")[2]) && !myAddressRefreshed.get()) {
                    //System.out.println("Refreshing my node list");
                    nodeList.refreshMyAddress(myAddress);
                    myAddressRefreshed.set(true);
                }
            } catch (RemoteException e) {
                offlineNodes.add(address);
            } catch (MalformedURLException | NotBoundException e) {
                e.printStackTrace();
            }
        }
    }

    //Calls announceLeave on the specified node
    private class AnnounceLeaveThread extends Thread {
        int key;

        public AnnounceLeaveThread(int key) {
            this.key = key;
        }

        @Override
        public void run() {
            String address = nodeList.addressMap.get(key);
            try {
                NodeInterface node = (NodeInterface) lookup(address);
                node.announceLeave(myKey);
            } catch (MalformedURLException | NotBoundException e) {
                e.printStackTrace();
            } catch (RemoteException ignored) {
            }
        }
    }

    //Calls updateNodeReplicas on the specified node
    private class UpdateReplicasThread extends Thread {
        String address;
        AtomicBoolean result;
        ConcurrentSkipListSet<String> offlineNodes;

        public UpdateReplicasThread(String address, AtomicBoolean result, ConcurrentSkipListSet<String> offlineNodes) {
            this.address = address;
            this.result = result;
            this.offlineNodes = offlineNodes;
        }

        @Override
        public void run() {
            try {
                //System.out.println("updateReplicasThread: " + address);
                NodeInterface node = (NodeInterface) lookup(address);
                boolean newResult = node.updateNodeReplicas(offlineNodes);
                result.set(result.get() && newResult);
            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                result.set(false);
                offlineNodes.add(address);
            }
        }
    }

    //Calls trimNodeReplicas on the specified node
    private class TrimReplicasThread extends Thread {
        String address;
        Set<String> offlineNodes;

        public TrimReplicasThread(String address, Set<String> offlineNodes) {
            this.address = address;
            this.offlineNodes = offlineNodes;
        }

        @Override
        public void run() {
            try {
                NodeInterface node = (NodeInterface) lookup(address);
                node.trimNodeReplicas(offlineNodes);
            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                offlineNodes.add(address);
            }
        }
    }

    //Returns the items that fall in the range of to the node who requested them
    private class UpdateNodeReplicasRunnable implements Runnable {
        String nodeAddress;
        int previousKey;
        CountDownLatch latch;
        Set<String> offlineNodes;
        AtomicInteger replies;
        Map<String, Map<Integer, VersionedValue>> replicasValues;

        public UpdateNodeReplicasRunnable(String nodeAddress, CountDownLatch latch, Set<String> offlineNodes,
                                          Map<String, Map<Integer, VersionedValue>> replicasValues, int previousKey, AtomicInteger replies) {
            this.nodeAddress = nodeAddress;
            this.latch = latch;
            this.offlineNodes = offlineNodes;
            this.previousKey = previousKey;
            this.replies = replies;
            this.replicasValues = replicasValues;
        }

        @Override
        public void run() {
            try {
                NodeInterface node = (NodeInterface) lookup(nodeAddress);
                replicasValues.put(nodeAddress, node.getKeysInRange(previousKey, myKey));
                replies.getAndIncrement();
            } catch (NotBoundException | MalformedURLException | RemoteException e) {
                offlineNodes.add(nodeAddress);
                System.out.println("in UpdateNodeReplicas: " + nodeAddress + " is offline");
            }
            latch.countDown();
        }
    }

    //Checks if the specified node has outdated items and in that case send the latest versions to it
    private class UpdateNodeReplicasWriteThread extends Thread {
        String nodeAddress;
        Map<Integer, VersionedValue> complete;
        Map<String, Map<Integer, VersionedValue>> replicasValues;

        public UpdateNodeReplicasWriteThread(String nodeAddress, Map<Integer, VersionedValue> complete, Map<String, Map<Integer, VersionedValue>> replicasValues) {
            this.nodeAddress = nodeAddress;
            this.complete = complete;
            this.replicasValues = replicasValues;
        }

        @Override
        public void run() {
            Map<Integer, VersionedValue> toSend = new ConcurrentSkipListMap<>(complete);
            toSend.entrySet().removeAll(replicasValues.get(nodeAddress).entrySet());
            if (toSend.size() > 0) {
                try {
                    NodeInterface node = (NodeInterface) lookup(nodeAddress);
                    node.writeAll(toSend);
                } catch (RemoteException | StorageFullException | NotBoundException | MalformedURLException ignored) {
                }
            }
        }
    }

    //Passes a list of items to the specified node
    private class SendMapThread extends Thread {
        String nodeAddress;
        Map<Integer, VersionedValue> map;

        public SendMapThread(String nodeAddress, Map<Integer, VersionedValue> map) {
            this.nodeAddress = nodeAddress;
            this.map = map;
        }

        @Override
        public void run() {
            try {
                NodeInterface node = (NodeInterface) lookup(nodeAddress);
                node.writeAll(map);
            } catch (RemoteException | StorageFullException | NotBoundException | MalformedURLException ignored) {
            }
        }
    }
}