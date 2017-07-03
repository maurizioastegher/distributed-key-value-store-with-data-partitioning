package ds;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public interface ClientInterface extends Remote {
    void returnResults(String result) throws RemoteException;
}
