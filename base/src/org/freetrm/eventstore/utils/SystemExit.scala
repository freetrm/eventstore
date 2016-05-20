package org.freetrm.eventstore.utils

/**
 * A sys.exit that doesn't print out all this information can be very painful to track down.
 */
object SystemExit {
  def apply(status: Int, message: String, t: Throwable = null): Nothing = {
    System.err.println(message)

    Option(t).foreach(_.printStackTrace())

    System.out.println("Exiting. Called from " + new Throwable().getStackTrace()(1))

    System.out.flush()
    System.err.flush()

    sys.exit(status)
  }

}
