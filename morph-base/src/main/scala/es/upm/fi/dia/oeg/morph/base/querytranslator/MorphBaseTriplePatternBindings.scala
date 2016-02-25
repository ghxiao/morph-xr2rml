package es.upm.fi.dia.oeg.morph.base.querytranslator

import com.hp.hpl.jena.graph.Node

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap

object MorphBaseTriplePatternBindings {

    var mappingDocument: R2RMLMappingDocument = null // set in MorphBaseQueryTranslator constructor

    /**
     * Check the compatibility between a term map and a triple pattern term.
     * See definition in https://hal.archives-ouvertes.fr/hal-01245883.
     */
    def compatible(termMap: R2RMLTermMap, tpTerm: Node): Boolean = {

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
    }

    /**
     * Check the compatibility between a RefObjectMap and a triple pattern term,
     * i.e. between the subject map of the parent triples map and triple pattern term
     */
    def compatible(termMap: R2RMLRefObjectMap, tpTerm: Node): Boolean = {

        if (termMap.termType.isDefined && !tpTerm.isBlank)
            // A RefObjectMap may only have a RDF collection or list term type, in that case 
            // it necessarily produces blank nodes
            false
        else {
            val parentTM = mappingDocument.getParentTriplesMap(termMap)
            compatible(parentTM.subjectMap, tpTerm)
        }
    }
}
