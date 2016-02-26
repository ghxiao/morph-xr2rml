package es.upm.fi.dia.oeg.morph.base.querytranslator

import com.hp.hpl.jena.graph.Node

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
/**
 * Implementation of the different functions to figure out which triples maps are bound to a triple pattern,
 * i.e. those triples maps that are possible candidates to generate RDF triples matching the triple pattern.
 *
 * See full definitions in https://hal.archives-ouvertes.fr/hal-01245883.
 */
object MorphBaseTriplePatternBindings {

    var mappingDocument: R2RMLMappingDocument = null // set in MorphBaseQueryTranslator constructor

    /**
     * Check the compatibility between a term map and a triple pattern term.
     *
     * Note in case of a RefObjectMap: if the RefObjectMap has a termType (xrr:RdfList, xrr:RdfBag etc.)
     * we could also verify that the members of the tpTerm (list/bag etc.) have a termType,
     * language tag or datatype compatible with the definition of the xxr:nestedTermType of the RefObjectMam.
     * We choose not to go there as we assume this shall be rare.
     *
     * @param tm an instance of R2RMLTermMap or R2RMLRefObjectMap
     * @return true if compatible
     * @throws MorphException if tm is neither an R2RMLTermMap or R2RMLRefObjectMap
     */
    def compatible(tm: Object, tpTerm: Node): Boolean = {

        if (tm.isInstanceOf[R2RMLTermMap]) {

            // ---- Case of an xR2RML TermMap
            val termMap = tm.asInstanceOf[R2RMLTermMap]
            if (tpTerm.isVariable())
                // A variable is always compatible with a term map
                true
            else {
                // Check type of tpTerm vs. termType of term map
                val termType = termMap.inferTermType
                var incompatible: Boolean =
                    (tpTerm.isLiteral && termType != Constants.R2RML_LITERAL_URI) ||
                        (tpTerm.isURI && termType != Constants.R2RML_IRI_URI) ||
                        (tpTerm.isBlank && (termType != Constants.R2RML_BLANKNODE_URI && !termMap.isRdfCollectionTermType))

                if (tpTerm.isLiteral) {
                    // Check language tag
                    val L = tpTerm.getLiteralLanguage
                    if (L == null || L.isEmpty())
                        incompatible = incompatible || (termMap.languageTag.isDefined)
                    else
                        incompatible = incompatible ||
                            (!termMap.languageTag.isDefined || (termMap.languageTag.isDefined && termMap.languageTag.get != L))

                    // Check data type
                    val T = tpTerm.getLiteralDatatypeURI
                    if (T == null || T.isEmpty())
                        incompatible = incompatible || (termMap.datatype.isDefined)
                    else
                        incompatible = incompatible ||
                            (!termMap.datatype.isDefined || (termMap.datatype.isDefined && termMap.datatype.get != T))
                }

                // Constant term map and the triple pattern term does not have the same value
                incompatible = incompatible ||
                    (termMap.isConstantValued && tpTerm.isLiteral && termMap.getConstantValue != tpTerm.getLiteralValue.toString) ||
                    (termMap.isConstantValued && tpTerm.isURI && termMap.getConstantValue != tpTerm.getURI)

                // Template term map and the triple pattern does not match the template string
                incompatible = incompatible ||
                    termMap.isTemplateValued && tpTerm.isLiteral && termMap.getTemplateValues(tpTerm.getLiteralValue.toString).isEmpty ||
                    termMap.isTemplateValued && tpTerm.isURI && termMap.getTemplateValues(tpTerm.getURI).isEmpty

                !incompatible
            }
        } else if (tm.isInstanceOf[R2RMLRefObjectMap]) {

            // ---- Case of an xR2RML RefObjectMap
            val termMap = tm.asInstanceOf[R2RMLRefObjectMap]
            if (tpTerm.isVariable())
                // A variable is always compatible with a term map
                true
            else if (termMap.termType.isDefined && !tpTerm.isBlank)
                // An xR2RML RefObjectMap may have a termType, but only a RDF collection or list term type. 
                // In that case, it necessarily produces blank nodes.
                false
            else {
                val parentTM = mappingDocument.getParentTriplesMap(termMap)
                compatible(parentTM.subjectMap, tpTerm)
            }
        } else
            throw new MorphException("Method compatible: object passed is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm)
    }

    /**
     * Check the compatibility between two term maps
     *
     * @param tm1 an instance of R2RMLTermMap or R2RMLRefObjectMap
     * @param tm2 an instance of R2RMLTermMap or R2RMLRefObjectMap
     * @return true if compatible
     * @throws MorphException if tm1 or tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap
     */
    def compatibleTermMaps(tm1: Object, tm2: Object): Boolean = {

        if (tm1.isInstanceOf[R2RMLTermMap]) {

            val termMap1 = tm1.asInstanceOf[R2RMLTermMap]
            if (tm2.isInstanceOf[R2RMLTermMap]) {
                // ---- tm1 and tm2 are TermMaps
                val termMap2 = tm2.asInstanceOf[R2RMLTermMap]
                var incompatible: Boolean =
                    (termMap1.inferTermType != termMap2.inferTermType) ||
                        (termMap1.languageTag != termMap2.languageTag) ||
                        (termMap1.datatype != termMap2.datatype) ||
                        (termMap1.isTemplateValued && termMap2.isTemplateValued &&
                            !TemplateUtility.compatibleTemplateStrings(termMap1.templateString, termMap2.templateString))

                !incompatible

            } else if (tm2.isInstanceOf[R2RMLRefObjectMap]) {
                // ---- tm1 is a TermMap and tm2 is a RefObjectMap
                val termMap2 = tm2.asInstanceOf[R2RMLRefObjectMap]
                compatibleTermMaps(termMap1, mappingDocument.getParentTriplesMap(termMap2))
            } else
                throw new MorphException("tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm2)

        } else if (tm1.isInstanceOf[R2RMLRefObjectMap]) {

            val termMap1 = tm1.asInstanceOf[R2RMLRefObjectMap]
            if (tm2.isInstanceOf[R2RMLTermMap]) {
                // ---- tm1 is a RefObjectMMap and tm2 is a TermMap
                val termMap2 = tm2.asInstanceOf[R2RMLTermMap]
                compatibleTermMaps(mappingDocument.getParentTriplesMap(termMap1), termMap2)

            } else if (tm2.isInstanceOf[R2RMLRefObjectMap]) {
                // ---- tm1 and tm2 are RefObjectMMaps
                val termMap2 = tm2.asInstanceOf[R2RMLRefObjectMap]
                compatibleTermMaps(mappingDocument.getParentTriplesMap(termMap1), mappingDocument.getParentTriplesMap(termMap2))
            } else
                throw new MorphException("tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm2)
        } else
            throw new MorphException("tm1 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm1)
    }
}
