package su.interference.mgmt;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface MgmtLink {
    String type();
    String retrieveMethod();
}
