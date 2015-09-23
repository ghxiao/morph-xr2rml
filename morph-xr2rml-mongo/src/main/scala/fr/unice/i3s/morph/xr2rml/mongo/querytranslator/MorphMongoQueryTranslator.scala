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
import com.hp.hpl.jena.reasoner.rulesys.builtins.IsLiteral
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import com.hp.hpl.jena.graph.Node
import es.upm.fi.dia.oeg.morph.base.querytranslator.SourceQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryConditionReference
import es.upm.fi.dia.oeg.morph.base.querytranslator.SourceQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphConditionType

/**
 * This class assumes that the xR2RML mapping is normalized, that is, a triples map
 * has not more that one predicate-object map, and each predicate-object map has
 * exactly one predicate and one object map.
 *
 * In the code this assumption is mentioned by the annotation @NORMALIZED_ASSUMPTION
 */
class MorphMongoQueryTranslator(val md: R2RMLMappingDocument) extends MorphBaseQueryTranslator {

    override val logger = Logger.getLogger(this.getClass());

    /**
     * High level method to start the translation process
     */
    override def translate(op: Op): UnionOfGenericQueries = {

        throw new Exception("Method not implemented")
    }

    /**
     * Translation of a triple pattern into a union of queries based on a candidate triples map.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for tp, that is a
     * triples map that should potentially be able to generate triples matching tp
     * @return @TODO
     */
    def transTP(tp: Triple, tm: R2RMLTriplesMap): UnionOfGenericQueries = {

        // Sanity checks about the @NORMALIZED_ASSUMPTION
        val poms = tm.getPropertyMappings
        if (poms.isEmpty || poms.size > 1)
            throw new MorphException("A candidate triples " + tm.toString + " map must have exactly one predicate-object map.")

        val pom = poms.head
        if (pom.predicateMaps.size != 1 && (pom.objectMaps.size != 1 || pom.refObjectMaps.size != 1))
            throw new MorphException("The candidate triples map  " + tm.toString + " must have exactly one predicate-object map.")

        val logSrc = tm.getLogicalSource
        if (logSrc.logicalTableType != Constants.LogicalTableType.QUERY)
            throw new MorphException("Logical source table type is not compatible with MongoDB")

        // Start translation
        val fromPart = genFrom(tm)
        val selecPart = genProjection(tp, tm)
        val wherePart = genCond(tp, tm)

        if (logger.isDebugEnabled())
            logger.debug("transTP(" + tp + ", " + tm + "):\n" +
                "fromPart:  " + fromPart + "\n" +
                "selecPart: " + selecPart + "\n" +
                "wherePart: " + wherePart + "\n")

        //throw new Exception("Method not implemented")
        null
    }

    /**
     * Generate the data sources from the triples map: that is, at least the query from the
     * logical source of the triples map passed as parameter (the child source), and optionally
     * the query of the logical source of the parent triples map in case there is a referencing object map.
     *
     * @return a map with the child query, respectively the parent query,
     * with map keys SourceQuery.Child, respectively SourceQuery.Parent.
     * Each query itself is an instance of MongoDBQuery;
     */
    def genFrom(tm: R2RMLTriplesMap): Map[SourceQuery.Value, MongoDBQuery] = {

        val logSrc = tm.getLogicalSource
        val childQuery = MongoDBQuery.parseQueryString(logSrc.getValue, logSrc.docIterator)
        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION

        if (pom.hasRefObjectMap) {
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
            val parentTMLogSrc = md.getParentTriplesMap(rom).getLogicalSource
            val parentQuery = MongoDBQuery.parseQueryString(parentTMLogSrc.getValue, parentTMLogSrc.docIterator)
            Map(SourceQuery.Child -> childQuery, SourceQuery.Parent -> parentQuery)
        } else
            Map(SourceQuery.Child -> childQuery)
    }

    /**
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the query.
     * This function is database-dependent:
     * in the case of MongoDB, references are JSONPath expressions, from which we can figure out which
     * fields to mention in the project part of the MongoDB query.
     *
     * This helps return only needed fields from JSON documents instead of complete documents.
     * Since MongoDB does not support joins, the join shall be processed by the xR2RML engine,
     * thus joined expressions must be projected too.
     * In addition, for each variable of tp, the corresponding term map must be projected.
     *
     * @return list of couples (Child, expression) or (Parent, expression),
     * where expression is a JSONPath reference from which named fields must be projected
     */
    def genProjection(tp: Triple, tm: R2RMLTriplesMap): List[(SourceQuery.Value, String)] = {

        var listRefs: List[(SourceQuery.Value, String)] = List.empty

        if (tp.getSubject().isVariable())
            listRefs = tm.subjectMap.getReferences.map(m => (SourceQuery.Child, m))

        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION
        if (tp.getPredicate().isVariable()) {
            listRefs = listRefs ::: pom.predicateMaps.head.getReferences.map(m => (SourceQuery.Child, m)) // @NORMALIZED_ASSUMPTION
        }

        if (pom.hasRefObjectMap) {
            // The joined fields must always be projected, whether tp.obj is an IRI or a variable: since MongoDB
            // cannot compute joins, the xR2RML processor has to do it, thus joined fields must be returned by both queries.
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION

            for (jc <- rom.joinConditions) {
                listRefs = listRefs :+ (SourceQuery.Child, jc.childRef) :+ (SourceQuery.Parent, jc.parentRef)
            }

            // In addition, if tp.obj is a variable, the subject of the parent TM must be projected too.
            if (tp.getObject().isVariable()) {
                val parentTM = md.getParentTriplesMap(rom)
                listRefs = listRefs ++ parentTM.subjectMap.getReferences.map(m => (SourceQuery.Parent, m)) // @NORMALIZED_ASSUMPTION
            }

        } else if (tp.getObject().isVariable())
            listRefs = listRefs ++ pom.objectMaps.head.getReferences.map(m => (SourceQuery.Child, m)) // @NORMALIZED_ASSUMPTION

        listRefs
    }

    /**
     * Generate the set of conditions to apply to the database query (the from part), so that triples map TM
     * generates triples that match tp.
     * If a triple pattern term is constant (IRI or literal), genCond generates an equality condition,
     * only if the term map is reference-valued or template-valued. Indeed, if the term map is constant-valued and
     * the tp term is constant too, we should already have checked that they were compatible in the bind_m function.
     *
     * If a triple pattern term is variable, genCond generates a not-null condition, only
     * if the term map is reference-valued or template-valued. Indeed, if the term map is constant-valued, then the variable
     * will simply be bound to the constant value of the term map but that does not entail any condition in the query.
     *
     * A conditions is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     *
     * @return set of conditions, either not-null, equality, or join in case of RefObjectMap.
     */
    def genCond(tp: Triple, tm: R2RMLTriplesMap): List[MorphBaseQueryCondition] = {

        var conditions: List[MorphBaseQueryCondition] = List.empty

        { // === Subject map ===
            val tpTerm = tp.getSubject
            val termMap = tm.subjectMap

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(SourceQuery.Child, ref))
                else if (tpTerm.isURI) {
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(SourceQuery.Child, termMap, tpTerm)
                }
        }
        { // === Predicate map ===
            val tpTerm = tp.getPredicate
            val termMap = tm.predicateObjectMaps.head.predicateMaps.head // @NORMALIZED_ASSUMPTION

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(SourceQuery.Child, ref))
                else if (tpTerm.isURI)
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(SourceQuery.Child, termMap, tpTerm)
        }
        { // === Object map ===
            val tpTerm = tp.getObject
            val pom = tm.predicateObjectMaps.head // @NORMALIZED_ASSUMPTION

            if (tpTerm.isLiteral) {
                // Add an equality condition for each reference in the term map
                val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                if (termMap.isReferenceOrTemplateValued)
                    conditions = conditions ++ genEqualityConditions(SourceQuery.Child, termMap, tpTerm)

            } else if (tpTerm.isURI) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION

                    // tp.obj is a constant IRI to be matched with the subject of the parent TM:
                    // add an equality condition for each reference in the subject map of the parent TM
                    val parentSM = md.getParentTriplesMap(rom).subjectMap
                    if (parentSM.isReferenceOrTemplateValued)
                        conditions = conditions ++ genEqualityConditions(SourceQuery.Parent, parentSM, tpTerm)

                    // Add the join conditions
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ MorphBaseQueryCondition.join(jc.childRef, jc.parentRef)
                } else {
                    // tp.obj is a constant IRI and there is no RefObjectMap: add an equality condition for each reference in the term map
                    val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ genEqualityConditions(SourceQuery.Child, termMap, tpTerm)
                }

            } else if (tpTerm.isVariable) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION

                    // tp.obj is a SPARQL variable to be matched with the subject of the parent TM
                    val parentSM = md.getParentTriplesMap(rom).subjectMap
                    if (parentSM.isReferenceOrTemplateValued)
                        conditions = conditions ++ parentSM.getReferences.map(ref => MorphBaseQueryCondition.notNull(SourceQuery.Parent, ref))

                    // Add the join conditions
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ MorphBaseQueryCondition.join(jc.childRef, jc.parentRef)
                } else {
                    // tp.obj is a SPARQL variable with no RefObjectMap: simply add a not-null condition for each reference in the term map
                    val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(SourceQuery.Child, ref))
                }
            }
        }

        conditions
    }

    /**
     * Generate one equality condition between a reference in the term map (Column name, JSONPath expression...)
     * and a term of a triple pattern.<br>
     * If the term map is reference-valued, one equality condition is generated.<br>
     * If the term map is template-value, possibly several equality conditions are generated, i.e. one for each
     * capturing group in the template string.
     *
     * Return an empty list for other types of term map.
     */
    private def genEqualityConditions(targetQuery: SourceQuery.Value, termMap: R2RMLTermMap, tpTerm: Node): List[MorphBaseQueryCondition] = {

        if (termMap.isReferenceValued) {
            // Make a new equality condition between the reference in the term map and the value in the triple pattern term
            List(MorphBaseQueryCondition.equality(targetQuery, termMap.getOriginalValue, tpTerm.toString))

        } else if (termMap.isTemplateValued) {
            // Get the references of the template string and the associated values in the triple pattern term
            val refValueCouples = TemplateUtility.getTemplateMatching(termMap.getOriginalValue, tpTerm.toString)

            // For each reference and associated value, build a new equality condition
            val refValueConds = refValueCouples.map(m => MorphBaseQueryCondition.equality(targetQuery, m._1, m._2))
            refValueConds.toList
        } else
            List.empty
    }
}