#!/bin/bash

set -e

duckdb ./tutorial.duckdb << EOF
  SELECT NULL;
EOF
