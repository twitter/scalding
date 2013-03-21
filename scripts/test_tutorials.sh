scripts/scald.rb --local tutorial/Tutorial0.scala || exit 1
scripts/scald.rb --local tutorial/Tutorial1.scala || exit 1
scripts/scald.rb --local tutorial/Tutorial2.scala || exit 1
scripts/scald.rb \
  --local tutorial/Tutorial3.scala \
  --input tutorial/data/hello.txt \
  || exit 1
scripts/scald.rb \
  --local tutorial/Tutorial4.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output4.txt \
  || exit 1
scripts/scald.rb \
  --local tutorial/Tutorial5.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output5.txt \
  --words tutorial/data/words.txt \
  || exit 1
scripts/scald.rb \
  --local tutorial/MatrixTutorial0.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/outdegree.tsv \
  || exit 1
scripts/scald.rb \
  --local tutorial/MatrixTutorial1.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cofollows.tsv \
  || exit 1
scripts/scald.rb \
  --local tutorial/MatrixTutorial2.scala \
  --input tutorial/data/graph.tsv \
  --maxOutdegree 1000 \
  --output tutorial/data/graphFiltered.tsv \
  || exit 1
scripts/scald.rb \
  --local tutorial/MatrixTutorial3.scala \
  --input1 tutorial/data/graph.tsv \
  --input2 tutorial/data/graph2.tsv \
  --intersection tutorial/data/intersection.tsv \
  --leftDiff tutorial/data/leftDiff.tsv \
  --rightDiff tutorial/data/rightDiff.tsv \
  || exit 1
scripts/scald.rb \
  --local tutorial/MatrixTutorial5.scala \
  --input tutorial/data/graph.tsv \
  --output tutorial/data/cosineSim.tsv \
  || exit 1
