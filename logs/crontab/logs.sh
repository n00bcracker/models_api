#!/bin/bash
CURRENT_DATE=$(date -I)
cd ../.. && /usr/bin/docker-compose logs -t --no-color | grep --color=never "$CURRENT_DATE" | sort -k 3 > logs/"$CURRENT_DATE".log