package fr.unice.i3s.morph.xr2rml.mongo.query

import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphConditionType

/**
 * MongoDB query element representing an equality or not null condition on a field.
 * For an equality:
 * 		$eq: value (non string value)
 *   or
 * 		$eq: "value" (string value)
 * For a not null condition:
 * 		$exists: true, $ne: null
 */
class MongoQueryNodeCond(val cond: MorphConditionType.Value, val value: Object) extends MongoQueryNode {

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeCond] && this.cond == q.asInstanceOf[MongoQueryNodeCond].cond && this.value == q.asInstanceOf[MongoQueryNodeCond].value
    }

    override def toQueryStringNotFirst() = {
        cond match {
            case MorphConditionType.IsNotNull => "$exists: true, $ne: null"

            case MorphConditionType.Equals =>
                if (value.isInstanceOf[String])
                    "$eq: '" + value + "'"
                else
                    "$eq: " + value
        }
    }
}
