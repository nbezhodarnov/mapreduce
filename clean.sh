#!/bin/bash

login="$1"

[[ -e index.html ]] && rm index.html

wget -q tp.telecom-paris.fr
ok_machines=$(cat index.html | grep "</tr>" | sed 's/<\/tr>/\n/g' | grep OK | grep -o tp-[1-9][a-z][0-9]*-[0-9]*  | awk '{print $1".enst.fr"}')

tmp=$(echo $ok_machines | tr "\n" " ")

machines=()

read -ra machines <<< "$tmp"

shuffled_machines=($(shuf -e "${machines[@]}"))

for machine in "${shuffled_machines[@]}"; do
  echo $machine
  ssh -q -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@$machine "exit"
  if [[ $? -eq 0 ]]
  then
    ssh -q -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@$machine "lsof -ti tcp:10326 | xargs kill -9"
    ssh -q -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@$machine "lsof -ti tcp:61375 | xargs kill -9"
    ssh -q -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@$machine "pgrep mapreducealg-master-node.jar | xargs kill -9"
  fi
done
