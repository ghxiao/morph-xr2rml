package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * A MongoQueryNodeCompare node in a abstract MongoDB query is used to represent a JavaScript filter coming from a JSONPath expression.
 * E.g.: In the JSONPath expression "$.p.q[?(@.r <= 5)]", the JavaScript part shall be translated into a piece if MongoDB
 * query condition like: "p.q.r: {$lte: 5}"
 */
class MongoQueryNodeCompare(val path: String, val operator: MongoQueryNodeCompare.Operator.Value, val value: String) extends MongoQueryNode {

    val dotNotedPath = MongoQueryNode.dotNotation(path)

    override def toQueryStringNotFirst() = {
        if (operator == MongoQueryNodeCompare.Operator.REGEX)
            "'" + dotNotedPath + "': {" + operator.toString() + ": /" + value + "/}"
        else
            "'" + dotNotedPath + "': {" + operator.toString() + ": " + value.replace("\"", "'") + "}"
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