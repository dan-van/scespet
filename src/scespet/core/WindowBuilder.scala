package scespet.core

import scespet.core.types._

/**
 * Created by danvan on 10/07/2014.
 * Oh, this may be overkill. Are they always hasVal[Boolean] ?
 */
trait WindowBuilder[-X] {
  def buildWindow(x:X, env:types.Env) :HasVal[Boolean]
}

object WindowBuilder {
  implicit object IdentityBuilder extends WindowBuilder[HasVal[Boolean]] {
    def buildWindow(x: HasVal[Boolean], env: Env): HasVal[Boolean] = x
  }
}
