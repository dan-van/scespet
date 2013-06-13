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
    public X value();
    public EventGraphObject getTrigger();
}
