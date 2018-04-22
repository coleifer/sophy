#!/bin/bash

for i in dist/*; do
  twine upload $i
done
