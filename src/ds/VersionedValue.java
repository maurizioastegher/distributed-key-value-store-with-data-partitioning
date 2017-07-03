package ds;

import java.io.Serializable;

/**
 * Created by:
 * 175195 Maurizio Astegher
 * 175185 Enrico Gambi
 */
public class VersionedValue implements Serializable {
    protected String value;
    protected int version;

    public VersionedValue(String value, int version) {
        this.value = value;
        this.version = version;
    }
}
