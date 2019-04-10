build:
	docker build -t beam .
	docker rm -v beam
	docker run --name beam -v $(shell pwd):/opt/beam -itd beam bash
	#docker exec -it beam bash cd /opt/beam/sdks/python/ && pip install -e .[test]
	#docker kill beam