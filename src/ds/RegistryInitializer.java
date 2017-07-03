package ds;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public class RegistryInitializer {
    public static void main(String args[]) {
        try {
            LocateRegistry.createRegistry(1099);
            System.in.read();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
