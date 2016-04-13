package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * A MongoQueryNodeCompare node in a abstract MongoDB query used to represent a JavaScript filter coming from a JSONPath expression.
 * E.g.: In the JSONPath expression <code>$.p.q[?(@.r <= 5)]</code>, the JavaScript part shall be translated into a piece of MongoDB
 * query condition like: <code>p.q.r: {\$lte: 5}</code>
 * 
 * @author Franck Michel, I3S laboratory
 */
class MongoQueryNodeCompare(val operator: MongoQueryNodeCompare.Operator.Value, val value: String) extends MongoQueryNode {

    override def equals(q: Any): Boolean = {
        if (q.isInstanceOf[MongoQueryNodeCompare]) {
            val qc = q.asInstanceOf[MongoQueryNodeCompare]
            this.operator == qc.operator && this.value == qc.value
        } else false
    }

    override def toString() = {
        if (operator == MongoQueryNodeCompare.Operator.REGEX)
            operator.toString() + ": /" + value
        else
            operator.toString() + ": " + value.replace("\"", "'")
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