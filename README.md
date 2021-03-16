# french-election-etl-example

## How to launch ?
First, build the containers, using:
```sh
docker-compose build
```
Then, launch `postgres` in background with:
```sh
docker-compose start postgres
```
Finally, run `spark-master` to launch the script:
```sh
docker-compose up spark-master
```
