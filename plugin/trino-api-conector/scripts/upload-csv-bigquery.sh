#!/bin/bash

# 1. Define your array of prefixes (and table names)
# You can add as many as you need here

PREFIXES=("lista-lead" "lista-protocolo" "lista-protocolo-status" "lista-atendimento" "lista-contato-chat" "lista-atendimento-chat" "lista-mensagem-atendimento-chat" "lista-atendimento-chatbot" "lista-mensagem-atendimento-chatbot")
#PREFIXES=("lista-protocolo")

# 2. Define your BigQuery Dataset
DATASET="lake_sae"

# 3. Loop through the array
for PREFIX in "${PREFIXES[@]}"; do
    echo "-----------------------------------------------"
    echo "Processing group: $PREFIX"
    
    # Check if any files exist for this prefix to avoid errors
    # This looks for files like sales*.csv
    FILES=$(ls ./csv/${PREFIX}-2*.csv 2>/dev/null)
    
    if [ -z "$FILES" ]; then
        echo " No files found for prefix: $PREFIX"
        continue
    fi

    # 4. Loop through the files found for this specific prefix
    for file in $FILES; do
        echo " Loading $file into ${DATASET}.${PREFIX}..."
        CLEAN_HEADER=$(head -n 1 "$file" | tr -d '"' | tr -d '\r')
        SCHEMA=$(echo "$CLEAN_HEADER" | sed 's/[^, ]*/&:STRING/g')
        
        bq load \
          --source_format=CSV \
          --encoding=UTF-8 \
          --skip_leading_rows=1 \
          --schema="$SCHEMA" \
          "${DATASET}.${PREFIX}_raw" \
          "$file"
          
        if [ $? -eq 0 ]; then
            echo " Successfully loaded $file"
        else
            echo " Error loading $file"
        fi
    done
done

echo "-----------------------------------------------"
echo "All groups processed."