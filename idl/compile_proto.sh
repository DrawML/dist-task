#!/usr/bin/env bash

OUTPUT_PATH=output

if [ -n "$1" ]
then
	output_path=$1
else
	output_path=$OUTPUT_PATH
fi

if [ ! -d "$output_path" ]
then
	echo "Make sure that '$output_path' directory is valid"
	exit -1;
fi

shopt -s nullglob
for proto_file in *.proto 
do
	echo "Compile $proto_file..."
	protoc --python_out=$output_path $proto_file
done
