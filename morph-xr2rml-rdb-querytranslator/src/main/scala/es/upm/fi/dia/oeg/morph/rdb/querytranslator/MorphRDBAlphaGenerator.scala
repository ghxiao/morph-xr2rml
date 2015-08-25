package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions.asJavaCollection

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseAlphaGenerator
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder

class MorphRDBAlphaGenerator(md: R2RMLMappingDocument, unfolder: MorphRDBUnfolder)
        extends MorphBaseAlphaGenerator(md, unfolder) {
    val databaseType = if (md.dbMetaData.isDefined) { md.dbMetaData.get.dbType; }
    else { Constants.DATABASE_DEFAULT }

    override val logger = Logger.getLogger("MorphQueryTranslator");

    override def calculateAlpha(tp: Triple, abstractConceptMapping: MorphBaseClassMapping, predicateURI: String): MorphAlphaResult = {
        //ALPHA SUBJECT
        val tpSubject = tp.getSubject();
        val alphaSubject = this.calculateAlphaSubject(tpSubject, abstractConceptMapping);
        val logicalTableAlias = alphaSubject.getAlias();

        val pmsAux = abstractConceptMapping.getPropertyMappings(predicateURI);
        val alphaResult: MorphAlphaResult = {
            if (RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
                val alphaPO = List((null, predicateURI));
                new MorphAlphaResult(alphaSubject, alphaPO);
            } else {
                val pms = {
                    if (pmsAux == null) { Nil }
                    else { pmsAux }
                }

                //ALPHA PREDICATE OBJECT
                val alphaPredicateObjects: List[(SQLJoinTable, String)] = {
                    if (pms.size > 1) {
                        val errorMessage = "Multiple mappings of a predicate is not supported.";
                        logger.error(errorMessage);
                    }

                    val pm = pms.iterator.next().asInstanceOf[R2RMLPredicateObjectMap];
                    val refObjectMap = pm.getRefObjectMap(0);
                    if (refObjectMap != null) {
                        val alphaPredicateObject = this.calculateAlphaPredicateObject(
                            tp, abstractConceptMapping, pm, logicalTableAlias);
                        List(alphaPredicateObject);
                    } else {
                        Nil;
                    }

                }

                new MorphAlphaResult(alphaSubject, alphaPredicateObjects);

            }
        }

        alphaResult;
    }

    override def calculateAlpha(tp: Triple, abstractConceptMapping: MorphBaseClassMapping, predicateURI: String, pm: MorphBasePropertyMapping): MorphAlphaResult = {
        null;
    }

    override def calculateAlphaPredicateObject(triple: Triple, abstractConceptMapping: MorphBaseClassMapping, abstractPropertyMapping: MorphBasePropertyMapping, logicalTableAlias: String): (SQLJoinTable, String) = {

        val pm = abstractPropertyMapping.asInstanceOf[R2RMLPredicateObjectMap];
        val refObjectMap = pm.getRefObjectMap(0);

        val result: SQLJoinTable = {
            if (refObjectMap != null) {
                //				val parentLogicalTable = refObjectMap.getParentLogicalTable().asInstanceOf[R2RMLLogicalTable];
                //val md = this.owner.getMappingDocument().asInstanceOf[R2RMLMappingDocument];
                val parentTriplesMap = md.getParentTriplesMap(refObjectMap);
                val parentLogicalTable = parentTriplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];

                if (parentLogicalTable == null) {
                    val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
                    logger.error(errorMessage);
                }

                val sqlParentLogicalTableAux = unfolder.unfoldLogicalSource(parentLogicalTable);
                val sqlParentLogicalTable = new SQLJoinTable(sqlParentLogicalTableAux, Constants.JOINS_TYPE_INNER, null);

                val sqlParentLogicalTableAuxAlias = sqlParentLogicalTableAux.generateAlias();
                this.owner.mapTripleAlias += (triple -> sqlParentLogicalTableAuxAlias);
                val joinQueryAlias = sqlParentLogicalTableAuxAlias;

                val joinConditions = refObjectMap.joinConditions;
                val onExpression = MorphRDBUnfolder.unfoldJoinConditions(
                    joinConditions, logicalTableAlias, joinQueryAlias, databaseType);
                if (onExpression != null) {
                    sqlParentLogicalTable.onExpression = onExpression;
                }

                sqlParentLogicalTable;
            } else {
                null
            }
        }

        val predicateURI = triple.getPredicate().getURI();
        (result, predicateURI);
    }

    override def calculateAlphaSubject(subject: Node, abstractConceptMapping: MorphBaseClassMapping): SQLLogicalTable = {
        val cm = abstractConceptMapping.asInstanceOf[R2RMLTriplesMap];
        val r2rmlLogicalTable = cm.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val sqlLogicalTable = unfolder.unfoldLogicalSource(r2rmlLogicalTable).asInstanceOf[SQLLogicalTable]

        val cmLogicalTableAlias = r2rmlLogicalTable.alias;
        val logicalTableAlias = {
            if (cmLogicalTableAlias == null || cmLogicalTableAlias.equals("")) {
                sqlLogicalTable.generateAlias();
            } else {
                cmLogicalTableAlias
            }
        }

        sqlLogicalTable.setAlias(logicalTableAlias);
        sqlLogicalTable.databaseType = this.databaseType;
        return sqlLogicalTable;
    }

    override def calculateAlphaPredicateObjectSTG(tp: Triple, cm: MorphBaseClassMapping, tpPredicateURI: String, logicalTableAlias: String): List[(SQLJoinTable, String)] = {

        val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
        val alphaPredicateObjects: List[(SQLJoinTable, String)] = {
            if (isRDFTypeStatement) {
                //do nothing
                Nil;
            } else {
                val pms = cm.getPropertyMappings(tpPredicateURI);
                if (pms != null && !pms.isEmpty()) {
                    val pm = pms.iterator.next().asInstanceOf[R2RMLPredicateObjectMap];
                    val refObjectMap = pm.getRefObjectMap(0);
                    if (refObjectMap != null) {
                        val alphaPredicateObject = this.calculateAlphaPredicateObject(tp, cm, pm, logicalTableAlias);
                        List(alphaPredicateObject);
                    } else {
                        Nil;
                    }
                } else {
                    if (!isRDFTypeStatement) {
                        val errorMessage = "Undefined mapping for : " + tpPredicateURI + " in : " + cm.toString();
                        logger.error(errorMessage);
                        Nil;
                    } else {
                        Nil;
                    }
                }
            }
        }

        alphaPredicateObjects;
    }
}
