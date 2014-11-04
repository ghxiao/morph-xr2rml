package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import Zql.ZConstant
import Zql.ZExpression
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUtility
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder

class MorphRDBCondSQLGenerator(md: R2RMLMappingDocument, unfolder: MorphRDBUnfolder)
  extends MorphBaseCondSQLGenerator(md, unfolder: MorphBaseUnfolder) {
  override val logger = Logger.getLogger("MorphCondSQLGenerator");

  override def genCondSQLPredicateObject(tp: Triple, alphaResult: MorphAlphaResult, betaGenerator: MorphBaseBetaGenerator, cm: MorphBaseClassMapping, pm: MorphBasePropertyMapping): ZExpression = {
    val tpObject = tp.getObject();
    val logicalTableAlias = alphaResult.alphaSubject.getAlias();

    val poMap = pm.asInstanceOf[R2RMLPredicateObjectMap];

    val refObjectMap = poMap.getRefObjectMap(0);
    val objectMap = poMap.getObjectMap(0);
    if (refObjectMap == null && objectMap == null) {
      val errorMessage = "no mappings is specified.";
      logger.error(errorMessage);
      null
    } else if (refObjectMap != null && objectMap != null) {
      val errorMessage = "Wrong mapping, ObjectMap and RefObjectMap shouldn't be specified at the same time.";
      logger.error(errorMessage);
    }

    val result2: ZExpression = {
      if (tpObject.isLiteral()) {
        if (refObjectMap == null && objectMap == null) {
          val errorMessage = "triple.object is a literal, but RefObjectMap is specified instead of ObjectMap";
          logger.error(errorMessage);
        }

        val objectLiteralValue = tpObject.getLiteral().getValue();

        if (objectMap != null) {
          val columnName = objectMap.getColumnName();
          if (columnName != null) {
            val columnNameWithAlias = {
              if (logicalTableAlias != null && !logicalTableAlias.equals("")) {
                logicalTableAlias + "." + columnName;
              } else {
                columnName
              }
            }

            val columnConstant = new ZConstant(columnNameWithAlias, ZConstant.COLUMNNAME);
            val objectLiteral = new ZConstant(objectLiteralValue.toString(), ZConstant.STRING);
            new ZExpression("=", columnConstant, objectLiteral);
          } else {
            null
          }
        } else {
          null
        }
      } else if (tpObject.isURI()) {
        if (refObjectMap == null && objectMap == null) {
          null
        } else if (refObjectMap != null && objectMap != null) {
          null
        } else if (objectMap != null && refObjectMap == null) {
          val uri = tpObject.getURI();
          val termMapType = objectMap.termMapType;
          objectMap.termMapType match {
            case Constants.MorphTermMapType.TemplateTermMap => {
              MorphRDBUtility.generateCondForWellDefinedURI(
                objectMap, cm, uri, logicalTableAlias)
            }
            case Constants.MorphTermMapType.ColumnTermMap => {
              val columnName = objectMap.getColumnName();
              val columnNameWithAlias = {
                if (logicalTableAlias != null) {
                  logicalTableAlias + "." + columnName;
                } else {
                  columnName
                }
              }

              val zConstantObjectColumn = new ZConstant(columnNameWithAlias, ZConstant.COLUMNNAME);
              val zConstantObjectURI = new ZConstant(uri.toString(), ZConstant.STRING);
              new ZExpression("=", zConstantObjectColumn, zConstantObjectURI);
            }
            case Constants.MorphTermMapType.ConstantTermMap => {
              //TODO
              null
            }
            case _ => {
              null
            }
          }
        } else if (refObjectMap != null && objectMap == null) {
          //val refObjectMapAlias = this.owner.getTripleAlias(tp);
          val parentTriplesMap = md.getParentTriplesMap(refObjectMap);
          val parentSubjectMap = parentTriplesMap.subjectMap;
          val parentLogicalTable = parentTriplesMap.logicalSource;
          val refObjectMapAlias = parentLogicalTable.alias;

          val uriCondition = MorphRDBUtility.generateCondForWellDefinedURI(
            parentTriplesMap.subjectMap, parentTriplesMap, tpObject.getURI(),
            refObjectMapAlias);

          val expressionsList = List(uriCondition);
          MorphSQLUtility.combineExpresions(expressionsList, Constants.SQL_LOGICAL_OPERATOR_AND);
        } else {
          null
        }
      } else if (tpObject.isVariable()) {
        null
      } else {
        null
      }
    }

    result2;
  }

  override def genCondSQLSubjectURI(tpSubject: Node, alphaResult: MorphAlphaResult, cm: MorphBaseClassMapping): ZExpression = {
    val subjectURI = tpSubject.getURI();
    val tm = cm.asInstanceOf[R2RMLTriplesMap];
    val subjectURIConstant = new ZConstant(subjectURI, ZConstant.STRING);
    val logicalTableAlias = alphaResult.alphaSubject.getAlias();
    val subjectTermMapType = tm.subjectMap.termMapType;

    val result2: ZExpression = {
      subjectTermMapType match {
        case Constants.MorphTermMapType.TemplateTermMap => {
          try {
            MorphRDBUtility.generateCondForWellDefinedURI(tm.subjectMap, tm, tpSubject.getURI(), logicalTableAlias);
          } catch {
            case e: Exception => {
              logger.error(e.getMessage());
              throw new Exception(e);
            }
          }
        }
        case Constants.MorphTermMapType.ColumnTermMap => {
          val subjectMapColumn = new ZConstant(tm.subjectMap.getColumnName(), ZConstant.COLUMNNAME);
          new ZExpression("=", subjectMapColumn, subjectURIConstant);
        }
        case Constants.MorphTermMapType.ConstantTermMap => {
          val subjectMapColumn = new ZConstant(tm.subjectMap.getConstantValue(), ZConstant.COLUMNNAME);
          new ZExpression("=", subjectMapColumn, subjectURIConstant);
        }
        case _ => {
          val errorMessage = "Invalid term map type";
          logger.error(errorMessage);
          throw new Exception(errorMessage);
        }
      }

    }

    result2;
  }

}