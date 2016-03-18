package fr.unice.i3s.morph.xr2rml.mongo.query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.ConditionType

abstract class MongoQueryNodeCond extends MongoQueryNode {
}

object MongoQueryNodeCondFactory {
    /**
     *  Create a condition node from a generic AbstractQueryCondition.
     *  
     *  Equals and IsNotNull are terminal conditions, i.e. they can be translated
     *  straight into a MongoDB condition appended after a field name, example:
     *  " 'field': {$eq: 10} "
     *   
     *  Conversely, IsNull and Or conditions are non terminal for MongoDB.
     *  Example: "IsNull($.field)" means that the field is either null or it does not exists. 
     *  MongoDB does not allow the following expression that could be produced straight away:
     *  
     *  "  'field': $or: [{$eq: null}, {$exists: false}] "
     *  
     *  Instead we have to produce:
     *   
     *  "  $or: [{'field': {$eq: null}}, {'field': {$exists: false}}] "
     *  
     *  This factory only deals with the first two cases. The next two will be handled in JsonPathToMongoTranslator.
     */
    def apply(cond: AbstractQueryCondition): MongoQueryNodeCond = {
        cond.condType match {
            case ConditionType.IsNotNull => new MongoQueryNodeCondNotNull

            case ConditionType.Equals => new MongoQueryNodeCondEquals(cond.asInstanceOf[AbstractQueryConditionEquals].eqValue)

            case _ => throw new MorphException("Condition type not supported: " + cond)
        }
    }
}
