package fr.unice.i3s.morph.xr2rml.mongo.query

import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType

/**
 * MongoDB query element representing an equality or not null condition on a field.
 * For an equality:
 * 		$eq: value (non string value)
 *   or
 * 		$eq: "value" (string value)
 * For a not null condition:
 * 		$exists: true, $ne: null
 */
class MongoQueryNodeCond(val cond: ConditionType.Value, val value: Object) extends MongoQueryNode {

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeCond] && this.cond == q.asInstanceOf[MongoQueryNodeCond].cond &&
            this.value == q.asInstanceOf[MongoQueryNodeCond].value
    }

    override def toQueryStringNotFirst() = {
        cond match {
            case ConditionType.IsNotNull => "$exists: true, $ne: null"

            case ConditionType.Equals =>
                if (value.isInstanceOf[String])
                    "$eq: '" + value + "'"
                else
                    "$eq: " + value
        }
    }
}

object MongoQueryNodeCond {
    /**
     *  Create a MongoQueryNodeCond from a generic MorphBaseQueryCondition
     */
    def apply(cond: MorphBaseQueryCondition): MongoQueryNodeCond = {
        cond.condType match {
            case ConditionType.IsNotNull => new MongoQueryNodeCond(ConditionType.IsNotNull, null)
            case ConditionType.Equals => new MongoQueryNodeCond(ConditionType.Equals, cond.eqValue)
            case ConditionType.SparqlFilter => throw new MorphException("No equivalent Mongo condition for a SPARQL filter condition")
        }
    }
}
