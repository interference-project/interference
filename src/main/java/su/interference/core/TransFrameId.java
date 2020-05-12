package su.interference.core;

import java.io.Serializable;

public class TransFrameId implements Serializable {
    private final long cframeId;
    private final long uframeId;
    private final long transId;

    public TransFrameId(long cframeId, long uframeId, long transId) {
        this.cframeId = cframeId;
        this.uframeId = uframeId;
        this.transId = transId;
    }

    public boolean equals(final Object obj) {
        final TransFrameId id = (TransFrameId)obj;
        if (this.cframeId == id.cframeId && this.uframeId == id.uframeId && this.transId == id.transId) { return true; }
        return false;
    }

    public int hashCode() {
        int hashCode = 1;
        hashCode = 31 * hashCode + Long.valueOf(cframeId).hashCode();
        hashCode = 31 * hashCode + Long.valueOf(uframeId).hashCode();
        hashCode = 31 * hashCode + Long.valueOf(transId).hashCode();
        return hashCode;
    }
    public long getCframeId() {
        return cframeId;
    }

    public long getUframeId() {
        return uframeId;
    }

    public long getTransId() {
        return transId;
    }
}
