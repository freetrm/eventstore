package org.freetrm.eventstore.utils

import org.slf4j.LoggerFactory

trait Log {
  protected val log = LoggerFactory.getLogger(this.getClass)
}