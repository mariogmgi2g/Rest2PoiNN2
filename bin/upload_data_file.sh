#!/bin/bash

root_path="/Users/administrador/Desktop/Sistemas de recomendación y deep learning/Rest2PoiNN/"
file="data/scraped data/pois data/Athens/reviews/Acropolis_Museum.parquet"

f_path="${root_path}${file}"

# Se elimina hace un par de splits y se elimina el último elemento
IFS="/" read -ra elements <<< "$file"
unset 'elements[${#elements[@]}-1]'
rel_upload_path=$(IFS="/"; echo "${elements[*]}")
root_upload_path="mariog@156.35.105.33:'/media/nas/mariog/Rest2PoiNN/"

upload_path="${root_upload_path}${rel_upload_path}/'"

# Comprueba si el archivo existe
# Hacer un double quote en las variables cuando se usen en comandos para evitar
# que bash las separe en varios tokens debido a white spaces
f_path="${root_path}${file}"
echo "$upload_path"


if ! [ -f "$f_path" ]; then 
    echo "File does NOT exist."
fi


scp "$f_path" "$upload_path"
