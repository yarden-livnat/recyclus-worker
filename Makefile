name = recyclus-worker
user = ylivnat

all : clean push

build:
	docker build -t $(user)/$(name) .


push: build
	docker push $(user)/$(name)


clean:
	docker image rm $(user)/$(name)
