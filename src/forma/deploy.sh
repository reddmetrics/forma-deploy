#! /bin/sh

# the usual ...
echo "lein clean"
lein clean

echo "lein deps"
lein deps

echo "lein uberjar"
lein uberjar

# move jar file to job tracker
echo "Copying uberjar to $2"
scp -i $1 forma-0.2.0-SNAPSHOT-standalone.jar hadoop@$2:

echo "Logging into $2"
ssh -i $1 hadoop@$2

# launch a new screen with logging enabled
screen -Lm

# launch a repl
hadoop jar forma-0.2.0-SNAPSHOT-standalone.jar clojure.main


