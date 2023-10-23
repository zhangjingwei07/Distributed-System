#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
script_path="$parent_path/m3-server.jar"

echo "script_path : $script_path"
echo "Port Number: $1";
echo "Cache Size: $2";
echo "Cache Strategy: $3";

ssh -n localhost nohup java -jar $script_path $1 $2 $3 -admin &

