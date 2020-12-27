package su.interference.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.DataChunk;
import su.interference.core.Instance;
import su.interference.core.SystemCleanUp;
import su.interference.persistent.FrameData;

import java.util.Map;

public class Call extends Meter implements CallMBean {

    private final static Logger logger = LoggerFactory.getLogger(Call.class);
    public Call(String name) {
        super(name);
    }

    public void forceCleanUp() {
        SystemCleanUp.forceCleanUp();
    }

    public void reportFrames() {
        for (Object entry : Instance.getInstance().getFramesMap().entrySet()) {
            final FrameData f = (FrameData) ((DataChunk) ((Map.Entry) entry).getValue()).getEntity();
            logger.info("frame table:id:used "+f.getObjectId()+":"+f.getFrameId()+":"+f.getUsed());
        }
    }

}
