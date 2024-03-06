#!/bin/bash

cd ..

git clone https://github.com/facebook/rocksdb.git --depth=4
cd rocksdb
git clone https://github.com/westerndigitalcorporation/zenfs plugin/zenfs
