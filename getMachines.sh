#!/bin/bash

login="$1"
requiredCount=$2
port=$3

[[ -e index.html ]] && rm index.html

wget -q tp.telecom-paris.fr
ok_machines=$(cat index.html | grep "</tr>" | sed 's/<\/tr>/\n/g' | grep OK | grep -o tp-[1-9][a-z][0-9]*-[0-9]*  | awk '{print $1".enst.fr"}')

tmp=$(echo $ok_machines | tr "\n" " ")

machines=()

read -ra machines <<< "$tmp"

shuffled_machines=($(shuf -e "${machines[@]}"))

working_count=0
working_machines=()

for machine in "${shuffled_machines[@]}"; do
  ssh -q -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@$machine "exit"
  [[ $? -eq 0 ]] && [[ -z "$(ssh -q $login@$machine "lsof -ti tcp:61375")" ]] && working_machines+=("$machine") && ((working_count++))
  [[ $working_count -eq $requiredCount ]] && break
done

echo ${working_machines[@]}
