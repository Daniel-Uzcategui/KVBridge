#!/bin/bash

# Si el script NO se está ejecutando bajo nohup, se reinicia a sí mismo con nohup
if [ -z "$LOGGED_BY_NOHUP" ]; then
    export LOGGED_BY_NOHUP=1
    nohup "$0" "$@" > app.log 2>&1 &
    echo "Proceso enviado a segundo plano con nohup (PID: $!)"
    exit 0
fi

# --- Tu comando real aquí ---
cd /home/daniel/qwencode
npm start
