package stub.gsa.esg.mekon.core;

import com.sun.org.apache.xml.internal.utils.ObjectVector;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/11/2012
 * Time: 23:28
 * To change this template use File | Settings | File Templates.
 */
public interface Environment {
    public boolean hasChanged(Object trigger);
    public <T> void addListener(Object source, EventGraphObject sink);
    public <T> void removeListener(Object source, EventGraphObject sink);
    public void wakeupThisCycle(Function target);
}
