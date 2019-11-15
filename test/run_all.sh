#!/bin/bash

# RUN FROM DIRECTORY: p4-aos/test/
# bash script to start workers and do our testing for us

# compile in source directory. 
# Skip make clean bc protobuf compile takes forever.
cd ~/code/class/aos/p4-mapreduce/src
rm mapreduce_impl.o
rm mapreduce.o
rm run_worker.o
rm mr_task_factory.o
make -j4
# compile in test directory
cd ~/code/class/aos/p4-mapreduce/test/
make clean
make -j4

# clear files in output directory
rm ~/code/class/aos/p4-mapreduce/test/output/*

# Start workers in background
./mr_worker localhost:50051 &
./mr_worker localhost:50052 &
./mr_worker localhost:50053 &
./mr_worker localhost:50054 &
./mr_worker localhost:50055 &
./mr_worker localhost:50056 &

echo "workers started, running demo now."

# Start mapreduce process
./mrdemo

# Kill worker processes
killall mr_worker
