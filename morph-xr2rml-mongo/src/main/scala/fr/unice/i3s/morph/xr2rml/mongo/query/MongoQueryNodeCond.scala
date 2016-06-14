package fr.unice.i3s.morph.xr2rml.mongo.query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.ConditionType

/**
 * @author Franck Michel, I3S laboratory
 *
 */
abstract class MongoQueryNodeCond extends MongoQueryNode {
}

object MongoQueryNodeCondFactory {
    
    /**
     * Create a MongoDB query condition node from an abstract query condition.
     * 
     * Example: Equals(10) is translated into the string <code>{\$eq: 10}</code> that is appended
     * after a field name: <code>'field': {\$eq: 10}</code>.<br>
     * <i>Equals</i> and <i>IsNotNull</i> are terminal conditions for MongoDB, i.e. they can be 
     * translated straight after the field name.
     *  
     * Conversely, <i>IsNull</i>, <i>Or</i> and <i>And</i> conditions are non terminal for MongoDB.<br> 
     * Example: "IsNull($.field)" means that the field either does not exist or is null.
     * MongoDB does not allow the following expression:<br>
     * <code>'field': \$or: [{\$eq: null}, {\$exists: false}]</code><br>
     * Instead we have to produce:<br>
     * <code>\$or: [{'field': {\$eq: null}}, {'field': {\$exists: false}}]</code>
     * 
     * This factory only deals with the first two cases.
     * The two latter are handled in JsonPathToMongoTranslator.trans().
     * 
     * @param cond an abstract query condition
     * @return a MongoDB query condition node
     * 
     * @todo Condition type SparqlFilter is not managed.
     */
    def apply(cond: AbstractCondition): MongoQueryNodeCond = {
        cond.condType match {
            case ConditionType.IsNotNull => new MongoQueryNodeCondNotNull

            case ConditionType.Equals => new MongoQueryNodeCondEquals(cond.asInstanceOf[AbstractConditionEquals].eqValue)

            case _ => throw new MorphException("Condition type not supported: " + cond)
        }
    }
}
