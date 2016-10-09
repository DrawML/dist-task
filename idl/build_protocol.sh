#!/usr/bin/env bash

PROJECT_DIR=$(dirname "$(pwd)")
PROTOCOL_MODULE="$PROJECT_DIR/src/dist_system/protocol/pb"
OUTPUT_PATH="$(pwd)"/output

if [ -n "$1" ]
then
	output_path=$1
else
	output_path=$OUTPUT_PATH
fi

if [ ! -d "$PROTOCOL_MODULE" ]
then
    echo "There is no 'pb' directory in protocol module."
    exit -1
fi

if [ ! -e "$output_path" ]
then
    mkdir "$output_path"
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
	protoc --python_out="$output_path" "$proto_file"
done


echo "Compile done"

cd "$output_path" || {
    echo "Where is '$output_path'?"
    exit -1
}

shopt -s nullglob
for output_file in *.py
do
    echo "Move '$output_file' to protocol module path..."
    mv "$output_file" "$PROTOCOL_MODULE"
done
