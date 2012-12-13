
#!/bin/bash
#export verbose="-v"
export verbose=""

export n="10001"
export TextZipFile=../1399-8.zip
export testDir=testDir
export DerbyDB=derbyTestDB1
export DerbyDB_Lucene=testindex6_derbyTestDB1_luceneIndex
export DerbyDB_BDB=testindex7_derbyTestDB1_bdb
export SPARQL_Lucene=testindex8_sparql_luceneIndex

rm -r $testDir
mkdir $testDir
cd $testDir

export CLASSPATH=../../../dist/lib/lusql.jar:../../lib/derby.jar

echo "Test 1: Integer to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:NO:WITH_POSITIONS_OFFSETS  -n ${n} -l testindex1   ${verbose}
echo " Test 1: Integer to Lucene: complete"

echo "Test 2: Integer to BDB"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:NO:WITH_POSITIONS_OFFSETS -n ${n} -l testindex2 -si ca.gnewton.lusql.driver.bdb.BDBDocSink -P id  ${verbose}
echo " Test 2: Integer to BDB:complete"


echo ""
echo "Test 3: BDB to BDB"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.bdb.BDBDocSource -n ${n} -c testindex2 -l testindex3  -I ANALYZED:NO:WITH_POSITIONS_OFFSETS  -si ca.gnewton.lusql.driver.bdb.BDBDocSink  -P id  ${verbose}
echo " Test 3: BDB to BDB: complete"

echo ""
echo "Test 4: BDB to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.bdb.BDBDocSource  -n ${n} -c testindex3 -l testindex4  -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  ${verbose}
echo " Test 4: BDB to Lucene: complete"

echo ""
echo "Test 5: Lucene to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.lucene.LuceneDocSource  -n ${n}  -c testindex4 -l testindex5  -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  ${verbose}
echo " Test 5: Lucene to Lucene: complete"

echo ""
echo "Test 6: Simple JDBC to Lucene"
echo " Creating Derby database: $DerbyDB" 
echo $CLASSPATH
time java ca.gnewton.lusql.test.CreateTestDB $DerbyDB 500 200 2 $TextZipFile

time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:${DerbyDB} -d org.apache.derby.jdbc.EmbeddedDriver   -q "select * from article" -m 50 -l $DerbyDB_Lucene   ${verbose}
echo " Test 6: complete"


echo "Test 7: Simple JDBC to BDB"
time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:${DerbyDB} -d org.apache.derby.jdbc.EmbeddedDriver   -q "select * from article" -m 50  -si ca.gnewton.lusql.driver.bdb.BDBDocSink -P ID -l ${DerbyDB_BDB}   ${verbose}
echo " Test 7: Simple JDBC to BDB:complete"

echo ""
echo "Test 8: Subquery JDBC to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:${DerbyDB} -d org.apache.derby.jdbc.EmbeddedDriver   -q "select id, title, contents from article" -Q "ID|select Author.name from articleAuthorJoin, author where articleAuthorJoin.articleId=@ and articleAuthorJoin.authorId=author.id" -m 50 -l $DerbyDB_Lucene   ${verbose}
echo " Test 8: complete"


echo ""
echo "Test 9: SPARQL to Lucene"
java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.sparql.SparQLDocSource  -pso "query=PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  SELECT ?cdi ?chemicalLabel ?diseaseLabel WHERE { ?cdi rdf:type <http://bio2rdf.org/ctd_vocabulary:Chemical-Disease-Association> .?cdi <http://bio2rdf.org/ctd_vocabulary:chemical> ?chemical .?chemical rdf:type <http://bio2rdf.org/ctd_vocabulary:Chemical> .?cdi <http://bio2rdf.org/ctd_vocabulary:disease> ?disease .?disease rdf:type <http://bio2rdf.org/ctd_vocabulary:Disease> .?chemical rdfs:label ?chemicalLabel .?disease rdfs:label ?diseaseLabel . }"  -pso endPointURL=http://s4.semanticscience.org:16004/sparql -l $SPARQL_Lucene
echo " Test 9: complete"


cd ..
rm -r $testDir







