#!/usr/bin/env bash

PROJECT_DIR="$(dirname "$(pwd)")"
PROTOCOL_MODULE="$PROJECT_DIR/src/dist_system/protocol/pb"
COMPILE_SCRIPT=compile_proto.sh
OUTPUT_PATH=output

if [ -n "$1" ]
then
	output_path=$1
else
	output_path=$OUTPUT_PATH
fi

if [ ! -e "$COMPILE_SCRIPT" ]
then
    echo "There is no compile script."
    exit -1
fi

if [ ! -d "$PROTOCOL_MODULE" ]
then
    echo "There is no 'pb' directory in protocol module."
    exit -1
fi

source "$COMPILE_SCRIPT" "$output_path" || {
    echo "Compiling fail"
    exit -1
}

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
