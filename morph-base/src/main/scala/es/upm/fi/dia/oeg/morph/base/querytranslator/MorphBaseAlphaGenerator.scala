package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

abstract class MorphBaseAlphaGenerator(md: R2RMLMappingDocument, unfolder: MorphBaseUnfolder) {
    var owner: MorphBaseQueryTranslator = null;

    val logger = Logger.getLogger(this.getClass());

    def calculateAlpha(tp: Triple, cm: R2RMLTriplesMap, predicateURI: String): MorphAlphaResult;

    def calculateAlpha(tp: Triple, cm: R2RMLTriplesMap, predicateURI: String, pm: R2RMLPredicateObjectMap): MorphAlphaResult;

    def calculateAlphaPredicateObject(tp: Triple, cm: R2RMLTriplesMap, pm: R2RMLPredicateObjectMap, logicalTableAlias: String): (SQLJoinTable, String);

    def calculateAlphaSubject(subject: Node, cm: R2RMLTriplesMap): SQLLogicalTable;

    def calculateAlphaSTG(triples: Iterable[Triple], cm: R2RMLTriplesMap): java.util.List[MorphAlphaResultUnion] = {
        var alphaResultUnionList: List[MorphAlphaResultUnion] = Nil;

        val firstTriple = triples.iterator.next();
        val tpSubject = firstTriple.getSubject();
        val alphaSubject = this.calculateAlphaSubject(tpSubject, cm);
        val logicalTableAlias = alphaSubject.getAlias();

        for (tp <- triples) {
            val tpPredicate = tp.getPredicate();
            var alphaPredicateObjects: List[(SQLJoinTable, String)] = Nil;
            var alphaPredicateObjects2: List[(SQLLogicalTable, String)] = Nil;
            if (tpPredicate.isURI()) {
                val tpPredicateURI = tpPredicate.getURI();

                val mappedClassURIs = cm.getMappedClassURIs();
                val processableTriplePattern = {
                    if (tp.getObject().isURI()) {
                        val objectURI = tp.getObject().getURI();
                        if (RDF.`type`.getURI().equals(tpPredicateURI) && mappedClassURIs.contains(objectURI) && triples.size > 1) {
                            false;
                        } else
                            true;
                    } else
                        true;
                }

                if (processableTriplePattern) {
                    val alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
                        tp, cm, tpPredicateURI, logicalTableAlias);
                    if (alphaPredicateObjectAux != null) {
                        alphaPredicateObjects = alphaPredicateObjects ::: alphaPredicateObjectAux.toList;
                    }

                    val alphaResult = new MorphAlphaResult(alphaSubject, alphaPredicateObjects);

                    val alphaTP = new MorphAlphaResultUnion(alphaResult);
                    alphaResultUnionList = alphaResultUnionList ::: List(alphaTP);
                }
            } else if (tpPredicate.isVariable()) {
                val pms = cm.getPropertyMappings();
                val alphaTP = new MorphAlphaResultUnion();
                for (pm <- pms) {
                    val tpPredicateURI = pm.getMappedPredicateName(0);
                    val alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
                        tp, cm, tpPredicateURI, logicalTableAlias);
                    if (alphaPredicateObjectAux != null) {
                        alphaPredicateObjects = alphaPredicateObjects ::: alphaPredicateObjectAux;
                    }

                    val alphaResult = new MorphAlphaResult(alphaSubject, alphaPredicateObjects);

                    alphaTP.add(alphaResult);
                }

                if (alphaTP != null) {
                    alphaResultUnionList = alphaResultUnionList ::: List(alphaTP);
                }

            } else {
                val errorMessage = "Predicate has to be either an URI or a variable";
                logger.error(errorMessage);

            }
        }

        return alphaResultUnionList;
    }

    def calculateAlphaPredicateObjectSTG(tp: Triple, cm: R2RMLTriplesMap, tpPredicateURI: String, logicalTableAlias: String): List[(SQLJoinTable, String)];
}

