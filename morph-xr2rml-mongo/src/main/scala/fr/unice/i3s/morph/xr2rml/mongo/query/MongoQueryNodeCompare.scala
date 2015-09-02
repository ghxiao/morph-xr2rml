package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * A MongoQueryNodeCompare node in a abstract MongoDB query is used to represent a Javascript filter coming from a JSONPath expression.
 * E.g.: In the JSONPath expression "$.p.q[?(@.r <= 5)]", the Javascript part shall be translated into a piece if MongoDB
 * query like: "p.q.r: {$lte: 5}"
 */
class MongoQueryNodeCompare(val mongoPath: String, val operator: MongoQueryNodeCompare.Operator.Value, val value: Object) extends MongoQueryNode {

    override def toQueryStringNotFirst() = {
        if (value.isInstanceOf[String])
            "'" + mongoPath + "': {" + operator.toString() + ": '" + value + "'}"
        else
            "'" + mongoPath + "': {" + operator.toString() + ": " + value + "}"
    }
}

object MongoQueryNodeCompare {
    object Operator extends Enumeration {
        val EQ = Value("$eq")
        val NE = Value("$ne")
        val LTE = Value("$lte")
        val LT = Value("$lt")
        val GTE = Value("$gte")
        val GT = Value("$gt")
        val REGEX = Value("$regex")
        val SIZE = Value("$size")
    }
}