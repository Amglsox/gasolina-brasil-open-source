IMAGE :=  pyspark-etl-image
path := lucas_mari

all: cp_conda zip_modules build delete_conda_sh

build:
	docker build . -f ./Dockerfile -t ${IMAGE}

cp_conda:
	curl https://repo.anaconda.com/miniconda/Miniconda3-py39_4.10.3-Linux-x86_64.sh > ./Miniconda3-py39_4.10.3-Linux-x86_64.sh

delete_jars:
	rm ./dataproc-serverless-docker-image/spark-bigquery-with-dependencies_2.12-0.22.2.jar

delete_conda_sh:
	rm ./dataproc-serverless-docker-image/Miniconda3-py39_4.10.3-Linux-x86_64.sh

zip_modules:
	zip -r ./../avenuecode_modules.zip modules
