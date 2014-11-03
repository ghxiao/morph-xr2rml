package es.upm.fi.dia.oeg.morph.base

import scala.collection.JavaConversions._
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Resource
import org.apache.log4j.Logger

class R2RMLMappingDocument(mappingFile: String) {
    val logger = Logger.getLogger(this.getClass());

    val XSDIntegerURI = XSDDatatype.XSDinteger.getURI();
    val XSDDoubleURI = XSDDatatype.XSDdouble.getURI();
    val XSDDateTimeURI = XSDDatatype.XSDdateTime.getURI();
    var datatypesMap: Map[String, String] = Map();
    datatypesMap += ("INT" -> XSDIntegerURI);
    datatypesMap += ("DOUBLE" -> XSDDoubleURI);
    datatypesMap += ("DATETIME" -> XSDDateTimeURI);

    val model = ModelFactory.createDefaultModel();
    val in = FileManager.get().open(mappingFile);
    model.read(in, null, "TURTLE");

    /**
     * Get all triples maps that generate resources of a given class IRI
     */
//    def getTriplesMapResourcesByRRClass(classURI: String) = {
//        var result: List[Resource] = Nil;
//        logger.info("method getTriplesMapResourcesByRRClass");
//        val triplesMapRes = model.listSubjectsWithProperty(Constants.R2RML_SUBJECTMAP_PROPERTY);
//        if (triplesMapRes != null) {
//            for (triplesMapRes <- triplesMapRes.toList()) yield {
//                val subjectMapRes = triplesMapRes.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY);
//                if (subjectMapRes != null) {
//                    val rrClassRes = subjectMapRes.getPropertyResourceValue(Constants.R2RML_CLASS_PROPERTY);
//                    if (rrClassRes != null) {
//                        if (classURI.equals(rrClassRes.getURI())) {
//                            result = result ::: result ::: List(triplesMapRes);
//                        }
//                    }
//                }
//            }
//        }
//        result;
//    }

    def getPredicateObjectMapResources(triplesMapResources: List[Resource]): List[Resource] = {
        val result = {
            if (triplesMapResources.isEmpty) {
                Nil; ;
            } else {
                val resultHead = this.getPredicateObjectMapResources(triplesMapResources.head);
                val resultTail = this.getPredicateObjectMapResources(triplesMapResources.tail);
                resultHead ::: resultHead ::: resultTail;
            }
        }
        result;
    }

    def getPredicateObjectMapResources(triplesMapResource: Resource): List[Resource] = {
        val pomStmtsIterator = triplesMapResource.listProperties(Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);
        val pomStmts = pomStmtsIterator.toList().toList;
        for (pomRes <- pomStmts) {
            val pomResObject = pomRes.getObject();
            val pomResObjectRes = pomResObject.asResource();
        }

        logger.info(" method getPredicateObjectMapResources ");
        val result = for { predicateObjectMapStatement <- pomStmts }
            yield predicateObjectMapStatement.getObject().asResource();
        result;
    }

    def getRRLogicalTable(triplesMapResource: Resource) = {
        triplesMapResource.getPropertyResourceValue(Constants.R2RML_LOGICALTABLE_PROPERTY);
    }

    def getRRLogicalSource(triplesMapResource: Resource) = {
        triplesMapResource.getPropertyResourceValue(xR2RML_Constants.xR2RML_LOGICALSOURCE_PROPERTY);
    }

    def getRRLogicalSourceRefFormulation(triplesMapResource: Resource) = {
        val refForm = this.getRRLogicalSource(triplesMapResource).getPropertyResourceValue(xR2RML_Constants.xR2RML_REFFORMULATION_PROPERTY);
        if (refForm != null)
            refForm.asLiteral().getValue().toString();
        else
            null;
    }

    def getRRSubjectMapResource(triplesMapResource: Resource) = {
        triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY);
    }

    def getRRLogicalTableTableName(triplesMapResource: Resource) = {
        val rrLogicalTableResource = this.getRRLogicalTable(triplesMapResource);
        val rrTableNameResource = rrLogicalTableResource.getPropertyResourceValue(Constants.R2RML_TABLENAME_PROPERTY);
        val result = {
            if (rrTableNameResource != null)
                rrTableNameResource.asLiteral().getValue().toString();
            else
                null;
        }
        result;
    }

    def getObjectMapResource(predicateObjectMapResource: Resource) = {

        val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
        val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
        val result: Resource = {
            if (parentTriplesMap == null) {
                objectMapResource
            } else {
                null;
            }
        }

        result;
    }

    def getRefObjectMapResource(predicateObjectMapResource: Resource) = {
        val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
        val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
        val result: Resource = {
            if (parentTriplesMap != null) {
                objectMapResource
            } else {
                null;
            }
        }

        result;
    }

    def getParentTriplesMapResource(objectMapResource: Resource) = {
        val parentTriplesMapResource = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
        parentTriplesMapResource;
    }

    def getParentTriplesMapLogicalTableResource(objectMapResource: Resource) = {
        logger.info(" method getParentTriplesMapLogicalTableResource ");
        val parentTriplesMapResource = this.getParentTriplesMapResource(objectMapResource);
        val parentTriplesMapLogicalTableResource = this.getRRLogicalTable(Constants.R2RML_LOGICALTABLE_PROPERTY);
        parentTriplesMapLogicalTableResource
    }

    def getTermTypeResource(termMapResource: Resource) = {
        val termTypeResource = termMapResource.getPropertyResourceValue(Constants.R2RML_TERMTYPE_PROPERTY);
        termTypeResource;
    }

    def getRRColumnResource(termMapResource: Resource) = {
        logger.info(" method getRRColumnResource ");
        val rrColumnResource = termMapResource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
        rrColumnResource;
    }

    def getRRTemplateResource(termMapResource: Resource) = {
        logger.info(" method getRRTemplateResource ");
        val rrTemplateResource = termMapResource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
        rrTemplateResource.getObject();
    }

    def getDatatypeResource(termMapResource: Resource) = {
        logger.info(" method getDatatypeResource ");
        val datatypeResource = termMapResource.getPropertyResourceValue(Constants.R2RML_DATATYPE_PROPERTY);
        datatypeResource;
    }

    def getTermMapType(tmRes: Resource) = {
        logger.info(" method getTermMapType ");
        if (tmRes == null) {
            logger.info("termMapResource is null");
        }

        val props = tmRes.listProperties().toList();

        val termMapType = {
            if (tmRes.getProperty(Constants.R2RML_CONSTANT_PROPERTY) != null)
                Constants.MorphTermMapType.ConstantTermMap;
            else if (tmRes.getProperty(Constants.R2RML_COLUMN_PROPERTY) != null)
                Constants.MorphTermMapType.ColumnTermMap;
            else if (tmRes.getProperty(Constants.R2RML_TEMPLATE_PROPERTY) != null)
                Constants.MorphTermMapType.TemplateTermMap;
            else if (tmRes.getProperty(xR2RML_Constants.xR2RML_REFERENCE_PROPERTY) != null)
                Constants.MorphTermMapType.ReferenceTermMap
            else
                Constants.MorphTermMapType.InvalidTermMapType;
        }
        termMapType;
    }

    def getTemplateValues(termMapResource: Resource, uri: String): Map[String, String] = {
        val termMapValueType = this.getTermMapType(termMapResource);

        val result: Map[String, String] = {
            if (termMapValueType == Constants.MorphTermMapType.TemplateTermMap) {
                val templateString = this.getRRTemplateResource(termMapResource).asLiteral().getValue().toString();
                val matchedTemplate = RegexUtility.getTemplateMatching(templateString, uri);
                matchedTemplate.toMap;
            } else {
                Map();
            }
        }
        result;
    }

}