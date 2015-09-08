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

    /**
     * Returns a MongoDB path consisting of a concatenation of single field names and array indexes in dot notation.
     * It removes the optional heading dot. E.g. dotNotation(.p[5]r) => p.5.r
     */
    def dotNotation(path: String): String = {
        var result =
            if (path.startsWith("."))
                path.substring(1)
            else path
        result = result.replace("[", ".").replace("]", "")
        result
    }
}