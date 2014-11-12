package es.upm.fi.dia.oeg.morph.base

import scala.util.matching.Regex
import scala.collection.JavaConversions._
import java.util.regex.Matcher
import java.util.regex.Pattern
import scala.collection.mutable.Queue
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath
import org.apache.log4j.Logger

object TemplateUtility {

    val logger = Logger.getLogger(this.getClass().getName())

    val TemplatePattern = Constants.R2RML_TEMPLATE_PATTERN.r

    val TemplatePatternCapGrp = Pattern.compile(Constants.R2RML_TEMPLATE_PATTERN_WITH_CAPTURING_GRP);

    def main(args: Array[String]) = {
        var template = "Hello {Name} Please find attached {Invoice Number} which is due on {Due Date}";

        val replacements = Map("Name" -> "Freddy", "Invoice Number" -> "INV0001")

        val attributes = TemplateUtility.getTemplateColumns(template);
        System.out.println("attributes = " + attributes);

        val template2 = TemplateUtility.replaceTemplateTokens(template, replacements);
        System.out.println("template2 = " + template2);

        template = """\{\w+\}""";
        val columnsFromTemplate = template.r.findAllIn("http://example.org/{abc}#{def}").toList;
        System.out.println("columnsFromTemplate = " + columnsFromTemplate);

        val date = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
        val dates = "Important dates in history: 2004-01-20, 1958-09-05, 2010-10-06, 2011-07-15"

        val firstDate = date findFirstIn dates getOrElse "No date found."
        System.out.println("firstDate: " + firstDate)

        val firstYear = for (m <- date findAllMatchIn dates) yield m group 1
        System.out.println("firstYear: " + firstYear.toList)
    }

    /**
     * CAUTION ### This method was not updated to support Mixed-syntax paths ###
     * Unused (at least in data materialization)
     */
    def getTemplateMatching(inputTemplateString: String, inputURIString: String): Map[String, String] = {

        println("#################### inputTemplateString " + inputTemplateString)
        var newTemplateString = inputTemplateString;
        if (!newTemplateString.startsWith("<"))
            newTemplateString = "<" + newTemplateString;
        if (!newTemplateString.endsWith(">"))
            newTemplateString = newTemplateString + ">";

        var newURIString = inputURIString;
        if (!newURIString.startsWith("<"))
            newURIString = "<" + newURIString;
        if (!newURIString.endsWith(">"))
            newURIString = newURIString + ">";

        val columnsFromTemplate = this.getTemplateColumns(newTemplateString);

        var columnsList = List[String]();
        for (column <- columnsFromTemplate) {
            columnsList = column :: columnsList;
            // Replace each group "{column}" with the regex "(.+?)"
            newTemplateString = newTemplateString.replaceAll("\\{" + column + "\\}", "(.+?)");
        }
        columnsList = columnsList.reverse;

        val pattern = new Regex(newTemplateString);
        val firstMatch = pattern.findFirstMatchIn(newURIString);

        // Create a map of couples (column name, column value)
        val result: Map[String, String] =
            if (firstMatch != None) {
                // Subgroups return values corresponding to the (.+?) groups
                val subgroups = firstMatch.get.subgroups;
                var i = 0;
                columnsList.map(column => {
                    val resultAux = (column -> subgroups(i));
                    i = i + 1;
                    resultAux
                }).toMap
            } else
                Map.empty

        result;
    }

    /**
     * Get the list of columns referenced in a template string.
     * This method applies to R2RML templates as well as xR2RML templates including
     * mixed-syntax paths like: "http://example.org/{Column(NAME)/JSONPath(...)}/...
     *
     * Extracting encapsulating groups at once with a regex is quite complicated due to mixed syntax paths
     * since they can contain '{' and '}'. As a result this method does this in several steps. We examplify this
     * on template string http://example.org/{ID}/{Column(NAME)/JSONPath(...)}
     * (1) Save all mixed-syntax paths from the template string to a list => List("Column(NAME)/JSONPath(...)").
     * (2) Replace each path expression with a place holder "xR2RML_replacer" => "http://example.org/{ID}/{xR2RML_replacer}".
     * (3) Extract all template groups between '{' and '}' to a list => List("{ID}", "{xR2RML_replacer}").
     * (4) Then, in this last list, we replace place holders with original mixed syntax path
     * expressions saved in step 1: List("{ID}", "{List("Column(NAME)/JSONPath(...)}").
     * (5) Finally, we extract the column name from each element of the list using the
     * MixedSyntaxPath construct => List("ID", "NAME")
     */
    def getTemplateColumns(tplStr: String): List[String] = {

        // (1) Save all mixed-syntax path expressions in the template string
        val mixedSntxRegex = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX
        val mixedSntxPaths: Queue[String] = Queue(mixedSntxRegex.findAllMatchIn(tplStr).toList.map(item => item.toString): _*)

        // (2) Replace each path expression with a place holder "xR2RML_replacer"
        val tpl2 = mixedSntxRegex.replaceAllIn(tplStr, "xR2RML_replacer")

        // (3) Make a list of the R2RML template groups between '{' '}'
        val listPattern = TemplatePattern.findAllIn(tpl2).toList

        // (4) Restore the path expressions in each of the place holders
        val listReplaced = MixedSyntaxPath.replaceTplPlaceholder(listPattern, mixedSntxPaths)

        // Extract the column references of each template group between '{' and '}'
        val columnsFromTemplate = listReplaced.map(group =>
            {
                val col = MixedSyntaxPath(group, xR2RML_Constants.xR2RML_COLUMN_URI).getReferencedColumn.getOrElse("")
                // For simple columns there has been no parsing at all so they still have the '{' and '}'
                if (col.startsWith("{") && col.endsWith("}"))
                    col.substring(1, col.length() - 1)
                else col
            }
        )
        logger.trace("Extracted referenced columns: " + columnsFromTemplate + " from template " + tplStr)
        columnsFromTemplate;
    }


    /**
     * Replace template capturing groups (tokens i.e. groups in { and }) with values
     * This method applies to R2RML templates as well as xR2RML templates including
     * mixed-syntax paths like: "http://example.org/{Column(NAME)/JSONPath(...)}/...
     *
     * Extracting encapsulating groups at once with a regex is quite complicated due to mixed syntax paths
     * since they can contain '{' and '}'. As a result this method does this in several steps:
     * (1) Save all mixed-syntax paths from the template string to a list => List("Column(NAME)/JSONPath(...)").
     * (2) Replace each path expression with a place holder "xR2RML_replacer" => "http://example.org/{ID}/{xR2RML_replacer}".
     * (3) Apply a pattern matcher to extract all template groups between '{' and '}'
     * (4) Then, in the template string, replace each token with its replacement value:
     * If a token equals "xR2RML_replacer", then we use the list saved in step 1 to get the mixed syntax
     * path and extract the column name from it.
     */
    def replaceTemplateTokens(tplStr: String, replacements: Map[String, Object]): String = {

        if (replacements.isEmpty) { tplStr }

        // (1) Save all mixed-syntax path expressions in the template string
        val mixedSntxRegex = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX
        val mixedSntxPaths = mixedSntxRegex.findAllMatchIn(tplStr).toList

        // (2) Replace each path expression with a place holder "xR2RML_replacer"
        val tpl2 = mixedSntxRegex.replaceAllIn(tplStr, "xR2RML_replacer")

        // (3) Make a list of the template groups between '{' '}'
        val matcher = TemplatePatternCapGrp.matcher(tpl2)

        var buffer: StringBuffer = new StringBuffer()
        var i = 0
        while (matcher.find()) {

            val path = matcher.group(1)
            val replacement =
                if (path.equals("xR2RML_replacer")) {
                    // If the current group is "xR2RML_replacer" then we have to take the original value
                    // that we saved in mixedSntxPaths,
                    val col = MixedSyntaxPath(mixedSntxPaths.get(i).toString, xR2RML_Constants.xR2RML_COLUMN_URI).getReferencedColumn.getOrElse("")
                    i = i + 1
                    replacements.get(col)
                } else {
                    // Otherwise, we are in the regular R2RML case with no mixed-syntax path
                    replacements.get(path)
                }
            if (!replacement.isDefined)
                logger.warn("In template " + tplStr + ", group " + path + " has no replacement.")
            else
                matcher.appendReplacement(buffer, replacement.get.toString)
        }
        matcher.appendTail(buffer);
        buffer.toString()
    }
}