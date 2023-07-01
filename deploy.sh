#!/bin/bash

login="${1:-"nbezkhod-22"}"
inputFileName="${2:-"/cal/commoncrawl/CC-MAIN-20230321002050-20230321032050-00486.warc.wet"}"
serversCount="${3:-6}"

mvn package

remoteFolder="/tmp/$login/"
masterNodeJarFileName="mapreducealg-master-node"
serverNodeJarFileName="mapreducealg-server-node"
fileExtension=".jar"

port="10326"

configurationFileName="configuration.txt"
testFileName="test.txt"

read -ra computers <<< "$(./getMachines.sh "$login" $(($serversCount + 1)) $port)"
#computers=("tp-5d02-01" "tp-5d02-03" "tp-5d02-04" "tp-5d02-05" "tp-5d02-06" "tp-5b07-40" "tp-5b07-38" "tp-5d02-11" "tp-5d02-12" "tp-5d02-19" "tp-5b07-38" "tp-5b07-37" "tp-5b07-36")
masterComputer="${computers[0]}"
computers=("${computers[@]:1}")

cat /dev/null > "$configurationFileName"
for c in ${computers[@]}; do
  echo "$c $port" >> "$configurationFileName"
  #command0=("ssh" "$login@$c" "[[ ! -z \"$(lsof -ti tcp:$port)\" ]] && lsof -ti tcp:$port | xargs kill -9")
  command0=("ssh" "$login@$c" "lsof -ti tcp:$port | xargs kill -9")
  command1=("ssh" "$login@$c" "rm -rf $remoteFolder;mkdir $remoteFolder")
  command2=("scp" "target/$serverNodeJarFileName$fileExtension" "$login@$c:$remoteFolder$serverNodeJarFileName$fileExtension")
  command3=("ssh" "$login@$c" "cd $remoteFolder;java -jar $serverNodeJarFileName$fileExtension $port")
  echo ${command0[*]}
  "${command0[@]}"
  echo ${command1[*]}
  "${command1[@]}"
  echo ${command2[*]}
  "${command2[@]}"
  echo ${command3[*]}
  "${command3[@]}" &
done

command1=("ssh" "$login@$masterComputer" "rm -rf $remoteFolder;mkdir $remoteFolder")
command2=("scp" "target/$masterNodeJarFileName$fileExtension" "$login@$masterComputer:$remoteFolder$masterNodeJarFileName$fileExtension")
command3=("scp" "$configurationFileName" "$login@$masterComputer:$remoteFolder$configurationFileName")
command4=("scp" "$testFileName" "$login@$masterComputer:$remoteFolder$testFileName")
command5=("ssh" "$login@$masterComputer" "cd $remoteFolder;java -jar $masterNodeJarFileName$fileExtension \"$inputFileName\"")
echo ${command1[*]}
"${command1[@]}"
echo ${command2[*]}
"${command2[@]}"
echo ${command3[*]}
"${command3[@]}"
echo ${command4[*]}
"${command4[@]}"
echo ${command5[*]}
"${command5[@]}"
