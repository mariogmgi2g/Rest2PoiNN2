#!/bin/bash

root_path="/Users/administrador/Desktop/Sistemas de recomendación y deep learning/Rest2PoiNN/"
folder="data/scraped data/pois data/Athens/images/Acropolis_Museum"

f_path="${root_path}${folder}/"

# Se elimina hace un par de splits y se elimina el último elemento
IFS="/" read -ra elements <<< "$folder"
unset 'elements[${#elements[@]}-1]'
rel_upload_path=$(IFS="/"; echo "${elements[*]}")
root_upload_path="mariog@156.35.105.33:'/media/nas/mariog/Rest2PoiNN/"

upload_path="${root_upload_path}${rel_upload_path}/'"

# Comprueba si el archivo existe
# Hacer un double quote en las variables cuando se usen en comandos para evitar
# que bash las separe en varios tokens debido a white spaces

echo "$upload_path"


if ! [ -f "$f_path" ]; then 
    echo "Root does NOT exist."
fi


scp -r "$f_path" "$upload_path"