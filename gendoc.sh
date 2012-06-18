#!/bin/bash
#
# Make sure you've installed codox!
#
# lein plugin install codox "0.3.2"

lein doc
cd doc
git checkout gh-pages
git add .
git commit -am "new documentation push."
echo "Pushing to gh-pages branch on github..."
git push -u origin gh-pages
cd ..

