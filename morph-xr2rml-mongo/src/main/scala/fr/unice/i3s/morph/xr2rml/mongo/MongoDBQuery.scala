package fr.unice.i3s.morph.xr2rml.mongo

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery

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
        val projection: String) {

    /** Constructor without projection */
    def this(collection: String, query: String) = this(collection, query, "")

    var iterator: Option[String] = None

    def setIterator(iter: Option[String]): MongoDBQuery = {
        this.iterator = iter
        this
    }

    override def toString(): String = {
        var str = "MongoDBQuery[Collection: " + collection + ". Query: " + query
        if (iterator.isDefined)
            str = str + ". Iterator: " + iterator.get
        if (!projection.isEmpty)
            str = str + ". Projection: " + projection
        str + "]"
    }

    override def equals(a: Any): Boolean = {
        val m = a.asInstanceOf[MongoDBQuery]
        this.collection == m.collection && GeneralUtility.cleanString(this.query) == GeneralUtility.cleanString(m.query) && {
            if (!this.iterator.isDefined || !m.iterator.isDefined)
                this.iterator == m.iterator
            else
                GeneralUtility.cleanString(this.iterator.get) == GeneralUtility.cleanString(m.iterator.get)
        }
    }
}

object MongoDBQuery {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Create a MongoDBQuery with no iterator.
     *
     * A query string looks like this: "db.myCollection.find({ 'a': { $exists: true} })".
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

    /**
     * Return the most specific logical source of q1 and q2, i.e. when one is a sub-query of the other.
     * Sub-query meaning that they have the same type, reference formulation and iterator,
     * and the query string of the one starts like the query string of the other.
     *
     * @example
     * q1 = db.collection.find({field1: 10}) and
     * q2 = db.collection.find({field1: 10, field2: 20}).
     * q2 is more specific than q1 =&gt; return an xR2RMLQuet with query string q2
     *
     * @param q1 an xR2RMLQuery with a MongoDB query string
     * @param q2 an xR2RMLQuery with a MongoDB query string
     * @return the most specific query if one is a sub-query of the other, or None otherwise.
     */
    def mostSpecificQuery(q1: xR2RMLLogicalSource, q2: xR2RMLLogicalSource): Option[xR2RMLQuery] = {

        if (q1.logicalTableType != q2.logicalTableType || q1.refFormulation != q2.refFormulation || q1.docIterator != q2.docIterator)
            return None
        if (!q1.isInstanceOf[xR2RMLQuery] || !q2.isInstanceOf[xR2RMLQuery])
            return None

        val mq1 = q1.asInstanceOf[xR2RMLQuery]
        val mq2 = q2.asInstanceOf[xR2RMLQuery]

        val q = mostSpecificQuery(mq1.query, mq2.query)
        if (q.isDefined)
            Some(new xR2RMLQuery(q.get, q1.refFormulation, q1.docIterator))
        else None
    }

    /**
     * Return the most specific query string of q1 and q2, i.e. when one is a sub-query of the other.
     * Sub-query meaning that they the query string of the one starts like the query string of the other.
     *
     * @example
     * q1 = db.collection.find({field1: 10}) and
     * q2 = db.collection.find({field1: 10, field2: 20}).
     * q2 is more specific than q1 =&gt; return Some(q2)
     *
     * @param q1 an MongoDB query string
     * @param q2 an MongoDB query string
     * @return the most specific query string if one is a sub-query of the other, or None otherwise.
     */
    private def mostSpecificQuery(q1: String, q2: String): Option[String] = {

        val mq1 = parseQueryString(GeneralUtility.cleanString(q1), true)
        val mq2 = parseQueryString(GeneralUtility.cleanString(q2), true)

        if (mq1.collection != mq2.collection)
            return None

        if (mq1.query.startsWith(mq2.query))
            return Some("db." + mq1.collection + ".find({" + mq1.query + "})")
        else if (mq2.query.startsWith(mq1.query))
            return Some("db." + mq2.collection + ".find({" + mq2.query + "})")
        else
            None
    }

    /**
     * Check if the left query is equal or more specific than the query,
     * i.e. they that they have the same type, reference formulation and iterator,
     * and the left query string starts like the right query string.
     *
     * @example
     * left  = db.collection.find({field1: 10, field2: 20}), and 
     * right = db.collection.find({field1: 10}).
     * left is more specific than right =&gt; return true
     *
     * @param left  an xR2RMLQuery with a MongoDB query string
     * @param right an xR2RMLQuery with a MongoDB query string
     * @return true if left is more specific than right
     */
    def isLeftMoreSpecific(left: xR2RMLLogicalSource, right: xR2RMLLogicalSource): Boolean = {

        if (left.logicalTableType != right.logicalTableType || left.refFormulation != right.refFormulation || left.docIterator != right.docIterator)
            return false
        if (!left.isInstanceOf[xR2RMLQuery] || !right.isInstanceOf[xR2RMLQuery])
            return false

        val mqLeft = parseQueryString(GeneralUtility.cleanString(left.asInstanceOf[xR2RMLQuery].query), true)
        val mqRight = parseQueryString(GeneralUtility.cleanString(right.asInstanceOf[xR2RMLQuery].query), true)

        (mqLeft.collection == mqRight.collection) && (mqLeft.query.startsWith(mqRight.query))
    }

}