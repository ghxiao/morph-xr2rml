package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * Abstract class to describe MongoDB queries
 */
abstract class MongoQueryNode {

    /** Is the current abstract node a field node? */
    def isField: Boolean = { false }

    def toQueryStringNotFirst(): String

    /** Build the concrete query string corresponding to that abstract query object */
    def toQueryString(): String = { toQueryStringNotFirst() }
}

object MongoQueryNode {
    object CondType extends Enumeration {
        val Equals, IsNotNull = Value
    }
}