lsof -i:$1 | awk '$0 ~ /LISTEN/ {print $2}' | xargs kill