
#!/bin/bash
export verbose="-v"
#export verbose=""

export CLASSPATH=../../../dist/lib/lusql.jar 
export testDir=testDir
export DerbyDB=derbyTestDB1
export DerbyDB_Lucene=derbyTestDB1_luceneIndex
export DerbyDB_BDB=derbyTestDB1_bdb

rm -r $testDir
mkdir $testDir
cd $testDir
echo ""
echo "Test 8: Subquery JDBC to Lucene"
echo " Creating Derby database: $DerbyDB" 
java ca.gnewton.lusql.test.CreateTestDB $DerbyDB

java -jar /home/gnewton/2011/lusql/lusql/dist/lib/lusql-0.96.jar  -c jdbc:derby:${DerbyDB} -d org.apache.derby.jdbc.EmbeddedDriver   -q "select id, title, contents from article" -Q "ID|select Author.name from articleAuthorJoin, author where articleAuthorJoin.articleId=@ and articleAuthorJoin.authorId=author.id" -m 50 -l $DerbyDB_Lucene   ${verbose}
echo " Test 8: complete"



cd ..
