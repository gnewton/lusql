#!/bin/bash
#export verbose="-v"
export CLASSPATH=../../../dist/lib/lusql.jar:../../lib/derby.jar

export verbose="-v"
export cleanup=0

export n="1000"
export TextZipFile=../1399-8.zip
export testDir=testDir

testNum=0

function start(){
 (( testNum++ ))
 echo "Test ${testNum}: $1"
}

function end(){
  echo "END"
  echo ""
}

printFirstN(){
  nprint=$1
  source=$2
  sourceDriver=$3
  echo "Printing first $nprint"
  echo "java ca.gnewton.lusql.core.LuSqlMain -n $nprint -so $sourceDriver  -c $source -si ca.gnewton.lusql.driver.file.PrintXMLDocSink "
  java ca.gnewton.lusql.core.LuSqlMain -n $nprint -so $sourceDriver  -c $source -si ca.gnewton.lusql.driver.file.PrintXMLDocSink -o
}

#############
rm -r $testDir &> /dev/null
mkdir $testDir
cd $testDir




start "Integer to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  -n ${n} -l testindex${testNum}_integer_lucene   ${verbose}
printFirstN 10 testindex${testNum}_integer_lucene ca.gnewton.lusql.driver.lucene.LuceneDocSource
end

start "Integer to CSV"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -n ${n} -l testindex${testNum}_integer_lucene   -si ca.gnewton.lusql.driver.csv.CSVPrintDocSink ${verbose} |gzip -c > testindex_integer_csv_${testNum}.csv.gzip 
end


start "Integer to BDB"
bdbNum=$testNum
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:NO:WITH_POSITIONS_OFFSETS -n ${n} -l testindex${testNum}_integer_bdb -si ca.gnewton.lusql.driver.bdb.BDBDocSink -P id  ${verbose}
end


start "Integer to Null"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:NO:WITH_POSITIONS_OFFSETS -n ${n} -l testindex${testNum}_integer_null -si ca.gnewton.lusql.driver.faux.NullDocSink ${verbose}
end

start "Integer to Serialize"
serializeNum=$testNum
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:NO:WITH_POSITIONS_OFFSETS -n ${n} -l testindex${testNum}_integer_serialize -si ca.gnewton.lusql.driver.serialize.SerializeDocSink ${verbose}
end

start "Serialize to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.serialize.SerializeDocSource -I ANALYZED:NO:WITH_POSITIONS_OFFSETS -n ${n} -c testindex${serializeNum}_integer_serialize  -l testindex${testNum}_serialize_lucene ${verbose}
end


start "BDB to BDB"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.bdb.BDBDocSource -n ${n} -c testindex${bdbNum}_integer_bdb -l testindex${testNum}_bdb_bdb  -I ANALYZED:NO:WITH_POSITIONS_OFFSETS  -si ca.gnewton.lusql.driver.bdb.BDBDocSink  -P id  ${verbose}
end

start "BDB to Lucene"
luceneNum=$testNum
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.bdb.BDBDocSource  -n ${n} -c testindex${bdbNum}_integer_bdb -l testindex${testNum}_bdb_lucene  -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  ${verbose}
end


start "Lucene to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.lucene.LuceneDocSource  -n ${n}  -c testindex${luceneNum}_bdb_lucene -l testindex${testNum}_lucene_lucene  -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  ${verbose}
end

start "Simple JDBC to Lucene"
jdbcNum=$testNum
echo " Creating Derby database: testindex${testNum}Derby" 
derbyNum=$testNum
time java ca.gnewton.lusql.test.CreateTestDB testindex${testNum}Derby 5000 200 2 $TextZipFile

time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${testNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select * from article" -m 50 -l testindex${testNum}_jdbc_lucene   ${verbose}
end


start "Simple JDBC to BDB"
time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${jdbcNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select * from article" -m 50  -si ca.gnewton.lusql.driver.bdb.BDBDocSink -P ID -l  testindex${testNum}_jdbc_bdb  ${verbose}
end


start "JDBC with Subquery to Lucene"
time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${jdbcNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select id, title, contents from article" -Q "ID|select Author.name from articleAuthorJoin, author where articleAuthorJoin.articleId=@ and articleAuthorJoin.authorId=author.id" -m 50 -l testindex${testNum}_jdbcSubquery_lucene   ${verbose}
end


start "SPARQL to Lucene"
java ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.sparql.SparQLDocSource  -pso "query=PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  SELECT ?cdi ?chemicalLabel ?diseaseLabel WHERE { ?cdi rdf:type <http://bio2rdf.org/ctd_vocabulary:Chemical-Disease-Association> .?cdi <http://bio2rdf.org/ctd_vocabulary:chemical> ?chemical .?chemical rdf:type <http://bio2rdf.org/ctd_vocabulary:Chemical> .?cdi <http://bio2rdf.org/ctd_vocabulary:disease> ?disease .?disease rdf:type <http://bio2rdf.org/ctd_vocabulary:Disease> .?chemical rdfs:label ?chemicalLabel .?disease rdfs:label ?diseaseLabel . }"  -pso endPointURL=http://s4.semanticscience.org:16004/sparql -l testindex${testNum}_sparql_lucene ${verbose}
end


start "JDBC to CSV"
time java ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${jdbcNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select id, title, contents from article" -Q "ID|select Author.name from articleAuthorJoin, author where articleAuthorJoin.articleId=@ and articleAuthorJoin.authorId=author.id" -m 50 -l testindex${jdbcNum}Derby ${verbose} -si ca.gnewton.lusql.driver.csv.CSVPrintDocSink |gzip -c > testindex${testNum}_sparql_csv.csv.gz ${verbose}
end


############################
cd ..
if [ $cleanup -eq 0 ]; then
	rm -r $testDir
fi








