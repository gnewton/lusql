#!/bin/bash
cd unjar
rm -r *

jar xf ../ant.jar			
jar xf ../apache-solr-common.jar	
jar xf ../apache-solr-solrj.jar	
jar xf ../commons-cli.jar		
jar xf ../commons-dbcp.jar	
jar xf ../commons-httpclient.jar	
jar xf ../commons-pool.jar  
jar xf ../javacsv.jar
jar xf ../je.jar		  
jar xf ../log4j.jar
jar xf ../lucene-core.jar     
jar xf ../mysql-connector-java-bin.jar
jar xf ../jena-arq.jar  
jar xf ../jena-core.jar
jar xf ../jena-iri.jar
jar xf ../xercesImpl-2.10.0.jar
jar xf ../xml-apis-1.4.01.jar
jar xf ../slf4j-api-1.6.4.jar
jar xf ../slf4j-log4j12-1.6.4.jar
jar xf ../json-simple.jar

