package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import scala.util.control.Exception
import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.UnionOfGenericQueries
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseProjectionGenerator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

/**
 * This class assumes that the xR2RML mapping is normalized, that is, a triples map
 * has not more that one predicate-object map, and each predicate-object map has
 * exactly one predicate and one object map.
 *
 * In the code this assumption is mentioned by the annotation @NORMALIZED_ASSUMPTION
 */
class MorphMongoQueryTranslator(val md: R2RMLMappingDocument) extends MorphBaseQueryTranslator {

    final val CHILDQ = "child"
    final val PARENTQ = "parent"

    override val logger = Logger.getLogger(this.getClass());

    /**
     * High level method to start the translation process
     */
    override def translate(op: Op): UnionOfGenericQueries = {

        throw new Exception("Method not implemented")
    }

    /**
     * Translation of a triple pattern into a union of queries based on a candidate triples map
     */
    def transTP(tp: Triple, tm: R2RMLTriplesMap): UnionOfGenericQueries = {

        // Sanity checks about the @NORMALIZED_ASSUMPTION
        val poms = tm.getPropertyMappings
        if (poms.isEmpty || poms.size > 1)
            throw new MorphException("A candidate triples " + tm.toString + " map must have exactly one predicate-object map.")
        val pom = poms.head
        if (pom.predicateMaps.size != 1 && (pom.objectMaps.size != 1 || pom.refObjectMaps.size != 1))
            throw new MorphException("The candidate triples map  " + tm.toString + " must have exactly one predicate-object map.")

        val fromPart = genFrom(tm)
        val selecPart = genProjection(tp, tm)
        val wherePart = genCond(tp, tm)

        throw new Exception("Method not implemented")
    }

    /**
     * Generate the data sources from the triples map: that is, at least the query from the
     * logical source of the triples map passed as parameter, and optionally the query
     * of the logical source of the parent triples map in case there is a referencing object map.
     *
     * @return the couple (childQuery, parentQuery) where childQuery is the MongoDBQuery of the triples map,
     * parentQuery is the optional MongoDBQuery of the parent triples map, if any
     */
    private def genFrom(tm: R2RMLTriplesMap): (MongoDBQuery, Option[MongoDBQuery]) = {
        val logSrc = tm.getLogicalSource
        if (logSrc.logicalTableType != Constants.LogicalTableType.QUERY)
            throw new MorphException("Logical source table type is not compatible with MongoDB")

        val childQuery = MongoDBQuery.parseQueryString(logSrc.getValue, logSrc.docIterator)

        val pom = tm.getPropertyMappings.head
        val parentQuery =
            if (pom.hasRefObjectMap) { // @NORMALIZED_ASSUMPTION
                val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
                val parentTMLogSrc = md.getParentTriplesMap(rom).getLogicalSource
                Some(MongoDBQuery.parseQueryString(parentTMLogSrc.getValue, parentTMLogSrc.docIterator))
            } else None
        (childQuery, parentQuery)
    }

    /**
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the query.
     * This function is database-dependent:
     * in the case of MongoDB, references are JSONPath expressions, from which we can figure out which
     * fields to mention in the project part of the MongoDB query.
     *
     * This helps return only needed fields from JSON documents instead of complete documents.
     * Since MongoDB does not support joins, joined expressions must be projected.
     * In addition, for each variable of tp, the corresponding term map must be projected.
     *
     * @return list of couples ("child", expression) or ("parent", expression),
     * where expression is JSONPath references from which named fields must be projected
     */
    private def genProjection(tp: Triple, tm: R2RMLTriplesMap): List[(String, String)] = {

        var listRefs: List[(String, String)] = List.empty

        if (tp.getSubject().isVariable())
            listRefs = tm.subjectMap.getReferences.map(m => (CHILDQ, m))

        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION
        if (tp.getPredicate().isVariable())
            listRefs = listRefs ::: pom.predicateMaps.head.getReferences.map(m => (CHILDQ, m))

        if (pom.hasRefObjectMap) {
            // The joined fields must always be projected, whether tp.obj is an IRI or a variable: since MongoDB
            // cannot compute joins, the xR2RML processor has to do it, thus joined fields must be returned by both queries.
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
            for (jc <- rom.joinConditions)
                listRefs = listRefs :+ (CHILDQ, jc.childRef) :+ (PARENTQ, jc.parentRef)

            // In addition, if tp.obj is a variable, the subject of the parent TM must be projected too.
            if (tp.getObject().isVariable())
                listRefs = listRefs ::: pom.objectMaps.head.getReferences.map(m => (PARENTQ, m)) // @NORMALIZED_ASSUMPTION

        } else if (tp.getObject().isVariable())
            listRefs = listRefs ::: pom.objectMaps.head.getReferences.map(m => (CHILDQ, m)) // @NORMALIZED_ASSUMPTION

        listRefs
    }

    /**
     * Generate the set of conditions to apply to the database query, so that triples map TM generates triples that match tp.
     * If a triple pattern term is constant (IRI or literal), genCond generates an equality condition,
     * only if the term map is reference-valued or template-valued. Indeed, if the term map is contant-valued and
     * the tp term is constant too, we have already checked that they were compatible in the bind_m function.
     * genCond generates a not-null condition in case the triple pattern term is a variable.
     * A conditions is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     */
    private def genCond(tp: Triple, tm: R2RMLTriplesMap): UnionOfGenericQueries = {
        
        
        
        
        throw new Exception("Method not implemented")
    }

    def getPRSQLGen(): MorphBaseProjectionGenerator = {
        throw new Exception("Method not implemented")
    }
}