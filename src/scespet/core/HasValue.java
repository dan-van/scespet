package scespet.core;

import gsa.esg.mekon.core.EventGraphObject;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 03/06/2013
 * Time: 20:44
 * To change this template use File | Settings | File Templates.
 */
public interface HasValue<X> {
    /**
     * return true if value is defined.
     * At this point I don't think a value should go undefined after becoming defined, as I worry that this would seriously complicate
     * implications for chained map reduce.
     * @return
     */
    public boolean initialised();
    public X value();
    public EventGraphObject getTrigger();
}
