package fr.unice.i3s.morph.xr2rml.mongo

import org.apache.log4j.Logger

/**
 * Simple representation of a MongoDB shell query:
 *
 * In query: db.collection.find({ 'a': { $exists: true} })
 * 'collection' is the collection name while "{ 'a': { $exists: true} }" is the query string
 *
 * Optionally, an iterator of the logical source can be set after object creation.
 */
class MongoDBQuery(
        val collection: String,
        val query: String,
        val projection: Option[String]) {

    /** Constructor without projection */
    def this(collection: String, query: String) = this(collection, query, None)

    var iterator: Option[String] = None

    def setIterator(iter: Option[String]): MongoDBQuery = {
        this.iterator = iter
        this
    }

    override def toString(): String = {
        var str = "MongoDBQuery[Collection: " + collection + ". Query: " + query
        if (iterator.isDefined)
            str = str + ". Iterator: " + iterator.get
        if (projection.isDefined)
            str = str + ". Projection: " + projection
        str + "]"
    }

    override def equals(a: Any): Boolean = {
        val m = a.asInstanceOf[MongoDBQuery]
        this.collection == m.collection && this.query == m.query && this.iterator == m.iterator
    }
}

object MongoDBQuery {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Create a MongoDBQuery with no iterator.
     * <br>
     * A query string looks like this: db.myCollection.find({ 'a': { $exists: true} })
     * This method return a MongoDBQuery instance where collection = myCollection
     * and query string = "{ 'a': { $exists: true} }"
     */
    def parseQueryString(query: String, stripCurlyBracket: Boolean): MongoDBQuery = {

        var tokens = query.trim.split("\\.")
        if (!tokens(0).equals("db")) {
            logger.error("Invalid query string: " + query)
            return null
        }
        val collection = tokens(1)

        // The query string starts after the '(' and finished before the trailing ')'
        tokens = query.split("\\(")
        var queryStr = tokens(1).substring(0, tokens(1).length - 1).trim

        if (stripCurlyBracket)
            if (queryStr.startsWith("{") && queryStr.endsWith("}"))
                queryStr = queryStr.substring(1, queryStr.length - 1).trim

        new MongoDBQuery(collection, queryStr)
    }

    /**
     * Create a MongoDBQuery and initialize the iterator.
     */
    def parseQueryString(query: String, iterator: Option[String], stripCurlyBracket: Boolean): MongoDBQuery = {
        parseQueryString(query, stripCurlyBracket).setIterator(iterator)
    }
}