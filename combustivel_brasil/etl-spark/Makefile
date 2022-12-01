IMAGE :=  pyspark-etl-image
path := lucas_mari

all: cp_conda zip_modules build delete_conda_sh

build:
	docker build . -f ./Dockerfile -t ${IMAGE}

zip_modules:
	zip -r ./modules.zip modules
