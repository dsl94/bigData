## Pre pokretanja
Zbog githuba nije moguce da se okaci veliki dataset fajl, skinuti ga sa linka ispod
https://drive.google.com/file/d/1rOAQpjIT_IWpuqFhTpTAFDFEZ71P0oq6/view?usp=sharing

## Podizanje env
### Pokrenuti 
`docker-compose up -d`

### Ubaciti dataset u hdfs

`docker cp oslo-bikes.csv namenode:/data`

`docker exec -it namenode bash`

`hdfs dfs -mkdir /dir`

`hdfs dfs -put /data/oslo-bikes.csv /dir`


## Pokretanje u clusteru
`docker build --rm -t bde/spark-app .`

`docker run --name oslo-bikes --net bde -p 4040:4040 -d bde/spark-app`
