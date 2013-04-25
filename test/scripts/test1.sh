#!/bin/bash
#export verbose="-v"
export CLASSPATH=../../../dist/lib/lusql.jar:../../lib/derby.jar
echo "ClASSPATH=${CLASSPATH}"

export verbose="-v"
export cleanup=1

export MINHEAPSIZE=512
export SMALLMAXHEAPSIZE=768
export MAXHEAPSIZE=11024

#export HEAP="-Xms256m -Xmx1024m -XX:+UseG1GC -XX:+AggressiveOpts -XX:+OptimizeStringConcat -XX:+UseCompressedOops "
#export HEAP="-Xms${MINHEAPSIZE} -Xmx${MAXHEAPSIZE}m    "
export HEAP="  -Xmx${MAXHEAPSIZE}m    "
export HEAP_SMALL=" -Xmx${SMALLMAXHEAPSIZE}m    "

export VMARGS=" ${HEAP} -XX:+AggressiveOpts "
export VMARGS_SMALL=" ${HEAP_SMALL} -XX:+AggressiveOpts "

export n="100"

export dirTime=`date +%Y.%m.%d.%k_%M_%S.%N`

export TextZipFile=../1399-8.zip
export testDir=testDir.${dirTime}

testNum=0

function start(){
	(( testNum++ ))
	echo "Test ${testNum}: $1"
}

function end(){
	if [ $cleanup -eq 0 ]; then
		until [ -z "$1" ]  # Until all parameters used up . . .
		do
			echo -n "$1 "
			shift
		done
	fi
	checkSuccess
	echo "END"
	echo ""
}

function checkSuccess(){
 if [ $? != 0 ]; then
	 # stop if something fails
	 echo "Exiting...."
	 exit -1
 fi
}

function printFirstN(){
	nprint=$1
	source=$2
	sourceDriver=$3
	echo "Printing first $nprint"
	echo "java ca.gnewton.lusql.core.LuSqlMain -n $nprint -so $sourceDriver  -c $source -si ca.gnewton.lusql.driver.file.PrintXMLDocSink "
	java ca.gnewton.lusql.core.LuSqlMain -n $nprint -so $sourceDriver  -c $source -si ca.gnewton.lusql.driver.file.PrintXMLDocSink -o
}

#############



runTest(){
	rm -r $testDir &> /dev/null
	mkdir $testDir
	cd $testDir

	# start "Integer to Null"
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -n ${n} -l testindex${testNum}_integer_null -si ca.gnewton.lusql.driver.faux.NullDocSink ${verbose}
	# end

	# start "Integer to Lucene"
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  -n ${n} -l testindex${testNum}_integer_lucene   ${verbose}
	# printFirstN 10 testindex${testNum}_integer_lucene ca.gnewton.lusql.driver.lucene.LuceneDocSource
	# end 

	# start "Integer to DocSourceCheckSink <-- Lucene"
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -n ${n} -l testindex${testNum}_integer_lucene   ${verbose} -si ca.gnewton.lusql.driver.sourcecheck.DocSourceCheckSink -psi so=ca.gnewton.lusql.driver.bdb.BDBDocSource -psi n=${n}
	# end testindex${testNum}_integer_lucene

	# start "Integer to CSV"
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -n ${n} -l testindex${testNum}_integer_lucene   -si ca.gnewton.lusql.driver.csv.CSVPrintDocSink ${verbose} |gzip -c > testindex_integer_csv_${testNum}.csv.gzip 
	# end testindex_integer_csv_${testNum}.csv.gzip 


	start "Integer to BDB"
	bdbNum=$testNum
	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource  -n ${n} -l testindex${testNum}_integer_bdb -si ca.gnewton.lusql.driver.bdb.BDBDocSink -P id  ${verbose}
	end

	# start "Integer to Serialize"
	# serializeNum=$testNum
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource  -n ${n} -l testindex${testNum}_integer_serialize -si ca.gnewton.lusql.driver.serialize.SerializeDocSink ${verbose}
	# end

	# start "Serialize to Lucene"
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.serialize.SerializeDocSource  -n ${n} -c testindex${serializeNum}_integer_serialize  -l testindex${testNum}_serialize_lucene ${verbose}
	# end


	# start "BDB to BDB"
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.bdb.BDBDocSource -n ${n} -c testindex${bdbNum}_integer_bdb -l testindex${testNum}_bdb_bdb  -si ca.gnewton.lusql.driver.bdb.BDBDocSink  -P id  ${verbose}
	# end

	# start "BDB to Lucene"
	# luceneNum=$testNum
	# time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.bdb.BDBDocSource  -n ${n} -c testindex${bdbNum}_integer_bdb -l testindex${testNum}_bdb_lucene  -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  ${verbose}
	# end


# 	start "Lucene to Lucene"
# 	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.lucene.LuceneDocSource  -n ${n}  -c testindex${luceneNum}_bdb_lucene -l testindex${testNum}_lucene_lucene  -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  ${verbose}
# 	end testindex${luceneNum}_bdb_lucene  testindex${testNum}_lucene_lucene

# 	start "Simple JDBC to Lucene"
# 	jdbcNum=$testNum
# 	derbyNum=$testNum
# 	echo " Creating Derby database: testindex${derbyNum}Derby" 
# 	time java $VMARGS ca.gnewton.lusql.test.CreateTestDB testindex${testNum}Derby 500 100 2 $TextZipFile

# 	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${testNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select * from article" -m 50 -l testindex${testNum}_jdbc_lucene   ${verbose}
# 	end


# 	start "Simple JDBC to BDB"
# # ID is capitalised because derby returns it as ID although it was created as "id"
# 	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${jdbcNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select * from article" -m 50  -si ca.gnewton.lusql.driver.bdb.BDBDocSink -P ID -l  testindex${testNum}_jdbc_bdb  ${verbose}
# 	end testindex${testNum}_jdbc_bdb


# 	start "JDBC with Subquery to Lucene"
# 	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${jdbcNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select id, title, contents from article" -Q "ID|select Author.name from articleAuthorJoin, author where articleAuthorJoin.articleId=@ and articleAuthorJoin.authorId=author.id" -m 50 -l testindex${testNum}_jdbcSubquery_lucene   ${verbose}
# 	end testindex${testNum}_jdbcSubquery_lucene


# 	start "JDBC to CSV"
# 	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain  -c jdbc:derby:testindex${jdbcNum}Derby -d org.apache.derby.jdbc.EmbeddedDriver   -q "select id, title, contents from article" -Q "ID|select Author.name from articleAuthorJoin, author where articleAuthorJoin.articleId=@ and articleAuthorJoin.authorId=author.id" -m 50 -l testindex${jdbcNum}Derby ${verbose} -si ca.gnewton.lusql.driver.csv.CSVPrintDocSink |gzip -c > testindex${testNum}_sparql_csv.csv.gz ${verbose}
# 	end

# 	start "Integer to http --> http to Lucene"

# 	echo "Starting http sink"
# 	time java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource -I ANALYZED:YES:WITH_POSITIONS_OFFSETS  -n ${n} -si ca.gnewton.lusql.driver.http.HttpDocSink ${verbose} &
# 	sleep 5
# 	checkSuccess
# 	echo "Starting http source 1"
# 	time java $VMARGS_SMALL ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.http.HttpDocSource  -n ${n} ${verbose} -l luceneHttp1& 
# 	echo "Starting http source 2"
# 	time java $VMARGS_SMALL ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.http.HttpDocSource  -n ${n} ${verbose} -l luceneHttp2& 
# 	echo "Starting http source 3"
# 	time java $VMARGS_SMALL ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.http.HttpDocSource  -n ${n} ${verbose} -l luceneHttp3& 
# 	echo "Starting http source 4"
# 	time java $VMARGS_SMALL ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.http.HttpDocSource  -n ${n} ${verbose} -l luceneHttp4& 
# 	echo "Starting http source 5"
# 	time java $VMARGS_SMALL ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.http.HttpDocSource  -n ${n} ${verbose} -l luceneHttp5& 
# 	wait

# 	end luceneHttp1 luceneHttp2 luceneHttp3 luceneHttp4 luceneHttp5

# 	start "SPARQL to Lucene"
# 	#java $VMARGS ca.gnewton.lusql.core.LuSqlMain -so ca.gnewton.lusql.driver.sparql.SparQLDocSource  -pso "query=PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  SELECT ?cdi ?chemicalLabel ?diseaseLabel WHERE { ?cdi rdf:type <http://bio2rdf.org/ctd_vocabulary:Chemical-Disease-Association> .?cdi <http://bio2rdf.org/ctd_vocabulary:chemical> ?chemical .?chemical rdf:type <http://bio2rdf.org/ctd_vocabulary:Chemical> .?cdi <http://bio2rdf.org/ctd_vocabulary:disease> ?disease .?disease rdf:type <http://bio2rdf.org/ctd_vocabulary:Disease> .?chemical rdfs:label ?chemicalLabel .?disease rdfs:label ?diseaseLabel . }"  -pso endPointURL=http://s4.semanticscience.org:16004/sparql -l testindex${testNum}_sparql_lucene ${verbose}
# 	end


############################
	cd ..
	if [ $cleanup -eq 0 ]; then
		rm -r $testDir
	fi
}

runTest







