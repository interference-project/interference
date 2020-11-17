package su.interference.metrics;

import su.interference.core.SystemCleanUp;

public class Call extends Meter implements CallMBean {

    public Call(String name) {
        super(name);
    }

    public void forceCleanUp() {
        SystemCleanUp.forceCleanUp();
    }

}
