package fr.unice.i3s.morph.xr2rml.mongo.query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.ConditionType

abstract class MongoQueryNodeCond extends MongoQueryNode {
}

object MongoQueryNodeCondFactory {
    /**
     * Create a MongoDB query condition node from a generic AbstractQueryCondition.
     * 
     * Equals and IsNotNull are terminal conditions, i.e. they can be translated
     * straight into a MongoDB condition appended after a field name.<br>
     * @example Equals(10) is translated into the string "{$eq: 10}" that is appended
     * after the field name.
     *  
     * Conversely, IsNull and Or conditions are non terminal for MongoDB.<br> 
     * Example: "IsNull($.field)" means that the field either does not exist or is null.
     * MongoDB does not allow the following expression:<br>
     * "  'field': $or: [{$eq: null}, {$exists: false}] "<br>
     * Instead we have to produce:<br>
     * "  $or: [{'field': {$eq: null}}, {'field': {$exists: false}}] "
     * 
     * This factory only deals with the first two cases.
     * The two latter will be handled in JsonPathToMongoTranslator.trans()
     * 
     * @todo Condition type SparqlFilter is not managed.
     */
    def apply(cond: AbstractQueryCondition): MongoQueryNodeCond = {
        cond.condType match {
            case ConditionType.IsNotNull => new MongoQueryNodeCondNotNull

            case ConditionType.Equals => new MongoQueryNodeCondEquals(cond.asInstanceOf[AbstractQueryConditionEquals].eqValue)

            case _ => throw new MorphException("Condition type not supported: " + cond)
        }
    }
}
