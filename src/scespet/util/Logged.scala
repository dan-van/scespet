package scespet.util

import java.util.logging.Logger

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/10/2013
 * Time: 22:18
 * To change this template use File | Settings | File Templates.
 */
trait Logged {
  val logger = Logger.getLogger(getClass.getName)
}
