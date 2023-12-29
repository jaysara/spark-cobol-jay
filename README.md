# Using Cobrix with java.

I ran in to some version mismatch issues while running this project in my IDE and through spark-submit.

* Use the pom.xml that is attached with this project.
* Make sure you download the latest build for `spark-cobo-2.6.5-bundle.jar` from [https://github.com/AbsaOSS/cobrix/releases/tag/v2.6.5](https://github.com/AbsaOSS/cobrix/releases/tag/v2.6.5)

* Make sure following command runs successfully,\
`mvn clean install exec:exec`

* You can submit the command through spark-submit by passing the location of the `spark-cobo-2.6.5-bundle.jar` that you downloaded.
* 
`spark-submit --class com.test.cobol.App --jars ~/Downloads/spark-cobol_2.12-2.6.5-bundle.jar --master local target/spark-cobol-jay-1.0-SNAPSHOT.jar
`
