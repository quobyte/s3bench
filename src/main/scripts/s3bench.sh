#!/bin/bash
# $1 - host
# $2 - bucket
# $3 - accessKeyId
# $4 - secret
# [$5 - configuration.properties]

# additional amazon client metrics: -Dcom.amazonaws.sdk.enableDefaultMetrics
java -cp lib/*:bin/* com.quobyte.s3bench.S3BenchmarkClient "$@"
