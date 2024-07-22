#!/bin/bash

sleep 20
python3 producer.py > "$LOGS_DIR"/producer.log 2>&1 &
python3 consumer.py > "$LOGS_DIR"/consumer.log 2>&1
