package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger

// The goal of this class is to convert a CSV data into JSON format
// The final result is a JSON array containing JSON documents made of the lines in the CSV data

object xR2RML_CSV_to_JSON {
    val logger = Logger.getLogger(this.getClass().getName());

    def convertCSVinJSON(data: String): String = {

        var csv = data.split("/n") // csv contains the lines of the CSV data., As the "\n" is wrongly interpreted in the database, 
        //we use here the "/n" to separate the different lines
        var ColumnNames = csv(0); // ColumnNames contains the fist line of the CSV data, namely the line that indicates the columns names

        var listofColumnNames = ColumnNames.split(",") // listofColumnNames is an array of the columns names, each value of listofColumnNames corresponds to a column name
        // and ach of these values will be used as a key in the jSON document
        var nbrColumns = 0;
        for (col <- listofColumnNames) { nbrColumns = nbrColumns + 1 }
        nbrColumns = nbrColumns - 1;
        var nbrlines = 0;
        for (col <- csv) { nbrlines = nbrlines + 1 }
        nbrlines = nbrlines - 1;

        var jsondata = "[";
        var boolean = true;
        for (l <- 0 to nbrlines) {
            // for each line except the first one, we'll create a JSON document
            if (boolean) { boolean = false /*  jump the first line*/ }
            else {

                var listofdata = csv(l).split(",")
                jsondata = jsondata + "{"
                for (a <- 0 to nbrColumns) {
                    jsondata = jsondata + "\"" + listofColumnNames(a) + "\":\"" + listofdata(a) + "\""
                    if (a != nbrColumns) { jsondata = jsondata + "," }
                }
                if (l != nbrlines) { jsondata = jsondata + "}," }
                else { jsondata = jsondata + "}" }
            }
        }

        jsondata = jsondata + "]";
        jsondata
    }
}