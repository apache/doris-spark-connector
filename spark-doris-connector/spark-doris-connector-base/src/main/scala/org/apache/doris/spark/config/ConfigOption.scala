package org.apache.doris.spark.config

class ConfigOption[T](val name: String, val defaultValue: Option[T], var description: Option[String] = None) {

  def withDescription(desc: String):ConfigOption[T] = {
    description = Some(desc)
    this
  }

}

object ConfigOption {

  class Builder[T](name: String) {

    def defaultValue(value: T) = new ConfigOption[T](name, Some(value))

    def withoutDefaultValue(): ConfigOption[T] = new ConfigOption[T](name, None)

  }

  def builder[T](name: String): Builder[T] = new Builder[T](name)

}