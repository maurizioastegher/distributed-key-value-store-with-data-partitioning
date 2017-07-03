package ds;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public class Client extends UnicastRemoteObject implements ClientInterface {
    public enum OpType {GET, UPDATE, UPDATE_REPLICAS, LEAVE, LIST}

    private static OpType opType;
    private String name;
    private int key;

    protected Client() throws RemoteException {
        super(0);
        try {
            LocateRegistry.createRegistry(1099);
        } catch (RemoteException ignored) {
        }
        Registry registry = LocateRegistry.getRegistry();
        //name = "Client" + Calendar.getInstance().getTimeInMillis();
        name = "Client" + System.nanoTime();
        registry.rebind(name, this); //Register the client on the RMI registry
    }

    public static void main(String args[]) {
        //Instantiate the client
        if (args.length < 2) {
            System.out.println(
                    "\nThis command is used to query an existing distributed key-value store.\n" +
                            "Please, specify the address of an existing node.\n" +
                            "Usage:\t\tjava ds.Client node_address operation [arguments]\n" +
                            "Examples:\tjava ds.Client //localhost/10 get 47\n" +
                            "\t\tjava ds.Client //localhost/20 update 47 \"my custom string\"\n" +
                            "\t\tjava ds.Client //localhost/30 leave\n" +
                            "\t\tjava ds.Client //localhost/10 updateReplicas\n");
            return;
        }
        try {
            Client client = new Client();
            client.doOperation(args);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    //Performs the requested operation on the node specified as argument
    public void doOperation(String args[]) {
        try {
            NodeInterface node = (NodeInterface) Naming.lookup(args[0]);
            if (args[1].equalsIgnoreCase("update")) {
                try {
                    opType = OpType.UPDATE;
                    key = Integer.parseInt(args[2]);
                    node.update(key, args[3], name);
                } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                    System.out.println("Invalid arguments.\nUsage:\tjava ds.Client node_address update key value");
                }
            } else if (args[1].equalsIgnoreCase("get")) {
                try {
                    opType = OpType.GET;
                    key = Integer.parseInt(args[2]);
                    node.get(key, name);
                } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                    System.out.println("Invalid arguments.\nUsage:\tjava ds.Client node_address get key");
                }
            } else if (args[1].equalsIgnoreCase("leave")) {
                opType = OpType.LEAVE;
                node.leave();
                System.out.println("Node " + args[0] + " left");
                exit();
            } else if (args[1].equalsIgnoreCase("list")) {
                opType = OpType.LIST;
                String list = node.listKeys();
                System.out.println("Items stored on node " + args[0] + ":");
                if (list.isEmpty()) {
                    System.out.println("<Empty>");
                } else {
                    System.out.println(list);
                }
                exit();
            } else if (args[1].equalsIgnoreCase("updateReplicas")) {
                opType = OpType.UPDATE_REPLICAS;
                node.updateAllReplicas(name);
            } else {
                System.out.println("Command not recognized");
            }
        } catch (NotBoundException e) {
            System.out.println("Node " + args[0] + " not found");
            exit();
        } catch (MalformedURLException e) {
            System.out.println("Malformed address. Example: //localhost/1");
            exit();
        } catch (RemoteException e) {
            System.out.println("Node not responding, please wait for the node to recover and retry");
            exit();
        }
    }

    //Unbinds the client from the registry and exits
    private void exit() {
        Registry registry;
        try {
            registry = LocateRegistry.getRegistry();
            registry.unbind(name);
        } catch (NotBoundException | RemoteException e) {
            e.printStackTrace();
        }

        //Wait for the method to return before terminating the process
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        }.start();
    }

    @Override
    //Prints the result of a requested operation
    public void returnResults(String result) throws RemoteException {
        switch (opType) {
            case UPDATE:
                System.out.println(result);
                break;
            case GET:
                if (result == null) {
                    System.out.println("Key not found");
                } else if (result.equals("FAIL")) {
                    System.out.println("FAIL: read quorum not reached");
                } else {
                    System.out.println("Key: " + key + "  \tValue: " + result);
                }
                break;
            case UPDATE_REPLICAS:
                if (result.equalsIgnoreCase("FAIL")) {
                    System.out.println("Some nodes could not be updated");
                } else {
                    System.out.println("OK");
                }
            default:
                break;
        }
        exit();
    }
}
