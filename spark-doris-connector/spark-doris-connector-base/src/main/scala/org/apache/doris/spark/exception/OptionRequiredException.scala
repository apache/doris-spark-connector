package org.apache.doris.spark.exception

class OptionRequiredException(name: String) extends Exception(s"option [$name] is required")
