rem Run xR2RML to convert TAXREF into SKOS

set XR2RMLJAR=C:\Users\fmichel\Documents\Development\eclipse-ws-xr2rml\morph-xr2rml\morph-xr2rml-dist
cd %XR2RMLJAR%
java -cp "target\morph-xr2rml-dist-1.0-SNAPSHOT-jar-with-dependencies.jar" ^
   fr.unice.i3s.morph.xr2rml.engine.MorphRunner ^
   --configDir example_mysql_rewriting
