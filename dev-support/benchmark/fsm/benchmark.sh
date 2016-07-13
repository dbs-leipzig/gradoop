#!/bin/bash

rm csv/fsm.csv

for F in series/*.conf
do
    cat fsm_common.conf ${F} > fsm.conf
    ./fsm.sh
done

rm fsm.conf
