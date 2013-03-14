scripts/scald.rb --local tutorial/Tutorial0.scala
scripts/scald.rb --local tutorial/Tutorial1.scala
scripts/scald.rb --local tutorial/Tutorial2.scala
scripts/scald.rb \
  --local tutorial/Tutorial3.scala \
  --input tutorial/data/hello.txt
scripts/scald.rb \
  --local tutorial/Tutorial4.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output4.txt
scripts/scald.rb \
  --local tutorial/Tutorial5.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output5.txt
scripts/scald.rb \
  --local tutorial/MatrixTutorial0.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/outdegree.tsv
scripts/scald.rb \
  --local tutorial/MatrixTutorial1.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cofollows.tsv
scripts/scald.rb \
  --local tutorial/MatrixTutorial2.scala \
  --input tutorial/data/graph.tsv \
  --maxOutdegree 1000 \
  --output tutorial/data/graphFiltered.tsv
scripts/scald.rb \
  --local tutorial/MatrixTutorial3.scala \
  --input1 tutorial/data/graph.tsv \
  --input2 tutorial/data/graph2.tsv \
  --intersection tutorial/data/intersection.tsv \
  --leftDiff tutorial/data/leftDiff.tsv \
  --rightDiff tutorial/data/rightDiff.tsv
scripts/scald.rb \
  --local tutorial/MatrixTutorial5.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cosineSim.tsv
