#!/bin/bash

# Ciudades de más interés -> [ (Rome_Lazio, 187791), (Madrid, 187514), (Paris, 187147), (Athens, 189400) ]

MAXTSTS=4

declare -a CITIES=("one_city")

for CITY in "${CITIES[@]}" ;do
    echo "-$CITY"
    i+=1
    # nohup /media/nas/mariog/Rest2PoiNN/.venv/bin/python -u  main.py "$CITY" > "./data/exe logs/download_$CITY.log" & 
    nohup '/Users/administrador/Desktop/Sistemas de recomendación y deep learning/Rest2PoiNN2/.venv/bin/python3' -u main.py "./data/cities to launch/$CITY.json" > "./data/exe logs/download_$CITY.log" &     
    # nohup '/Users/administrador/Desktop/Sistemas de recomendación y deep learning/Rest2PoiNN2/.venv/bin/python3' -u "./src/scraper/main.py" > "./data/scraping/scraping.log" &   
    # Si se alcanza el máximo de procesos simultaneos, esperar
    while [ $(jobs -r | wc -l) -eq $MAXTSTS ];
    do
      sleep 5
    done

done

# Esperar por los últimos
while [ $(jobs -r | wc -l) -gt 0 ];
do
  sleep 5
done