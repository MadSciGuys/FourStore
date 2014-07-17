#!/bin/bash

cabal clean
cabal configure
cabal install
cabal configure
cabal haddock --hyperlink-sources
cp -r dist/doc/html/FourH/ /data/drupal_web/sites/default/files/codebase_docs/
chmod --recursive 0755 /data/drupal_web/sites/default/files/codebase_docs/
