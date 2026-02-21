#!/bin/bash

mkdir ./csv
mkdir ./json

dataHoje=$(date +%Y-%m-%d)

endpointArr=("lista-lead" "lista-protocolo" "lista-protocolo-status" "lista-atendimento" "lista-contato-chat" "lista-atendimento-chat" "lista-mensagem-atendimento-chat" "lista-atendimento-chatbot" "lista-mensagem-atendimento-chatbot")
#endpointArr=("lista-lead")

dataInicio="2020-03-20"
#dataInicio="2023-09-21"

dataFinal=$dataHoje
#dataFinal="2023-10-22"

periodoDias=31
#periodoDias=10

generate_token(){
    if [ -z $tokenUNE ]; then
        tokenUNE=$(curl -s -H "Content-Length: 0" -X POST "https://crm.api.une.cx/api/auth/univesp/key?code=ccukletneonuhfelkejiuqqussufwu" | jq -r '.token')
    fi
}

generate_post_data()
{
  cat <<EOF
{
    "data_inicio": "$dataInicioTemp",
    "data_termino": "$dataFinalTemp"
}
EOF
}

query_endpoint_periodo(){
    JsonFileName=$endpoint-$dataInicioTemp-$dataFinalTemp.json
    CsvFileName=$endpoint-$dataInicioTemp-$dataFinalTemp.csv

    tempJsonFileName="jsonQuery.json"

    curl -s -H "Authorization: Bearer $tokenUNE" -H "Content-Type: application/json" -d "$(generate_post_data)" -X POST "https://crm.api.une.cx/api/univesp/$endpoint" > $tempJsonFileName

    echo "consulta ok"
    
    if [[ -z $json ]]; then
        unset tokenUNE
        generate_token
        curl -s -H "Authorization: Bearer $tokenUNE" -H "Content-Type: application/json" -d "$(generate_post_data)" -X POST "https://crm.api.une.cx/api/univesp/$endpoint" > $tempJsonFileName
    fi
    
    echo $JsonFileName

    tempJsonParsedFileName="temp.json"

    jq -r 'select(.success == true).data' $tempJsonFileName > $tempJsonParsedFileName
    rm $tempJsonFileName
    mv $tempJsonParsedFileName ./json/$JsonFileName
    jq -r '(.[0] | keys_unsorted) as $keys | $keys, (.[] | [.[$keys[]] | if type=="string" then gsub("\r"; "") | gsub("\n"; "\\n") else . end]) | @csv' $JsonFileName > ./csv/$CsvFileName
}

query_endpoint() {
    unset json
    unset jsonTemp

    dataInicioTemp=$dataInicio
    dataFinalTemp=$(date -d "$dataInicio + $periodoDias days" +"%Y-%m-%d")

    if [[ $periodoDias -gt 31 ]]; then
        periodoDias=31
    fi

    echo $endpoint

    while [[ "$(date -d "$dataInicioTemp" +%s)" -lt "$(date -d "$dataFinal" +%s)" ]];
    do
        generate_post_data
        query_endpoint_periodo

        dataInicioTemp=$(date -d "$dataFinalTemp + 1 days" +"%Y-%m-%d")
        dataFinalTemp=$(date -d "$dataInicioTemp + $periodoDias days" +"%Y-%m-%d")
    done
}

query_une_periodo(){

    for endpoint in "${endpointArr[@]}"; do
        query_endpoint
    done

}

generate_token

query_une_periodo
