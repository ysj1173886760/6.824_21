#!/bin/bash

rm res -rf
mkdir res
for ((i = 0; i < 200; i++))
do
echo $i
(go test) > ./res/$i
grep -nr "FAIL.*" res
done