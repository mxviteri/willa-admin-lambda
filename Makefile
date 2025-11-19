.PHONY: build package deploy deploy-s3 deploy-lambda

build:
	rm -rf build function.zip
	mkdir build
	pip install -r requirements.txt -t build
	cp -r *.py willa_rest_api willa_admin_agent build
	cd build && zip -r ../function.zip . && cd ..

deploy-s3-beta:
	aws s3 cp function.zip s3://willa-lambda-code-305780340534/admin-lambda-deployment.zip --profile willa-beta

deploy-s3-prod:
	aws s3 cp function.zip s3://willa-lambda-code-567736086349/admin-lambda-deployment.zip --profile willa-prod

deploy-lambda-beta:
	aws lambda update-function-code \
	  --function-name AdminStack-AdminHandler935B2997-7qMEhHa0e2Us \
	  --s3-bucket willa-lambda-code-305780340534 \
	  --s3-key admin-lambda-deployment.zip \
	  --profile willa-beta \
	  --no-cli-pager

deploy-lambda-prod:
	aws lambda update-function-code \
	  --function-name AdminStack-AdminHandler935B2997-c2o1m58BKOO4 \
	  --s3-bucket willa-lambda-code-567736086349 \
	  --s3-key admin-lambda-deployment.zip \
	  --profile willa-prod \
	  --no-cli-pager

deploy: build deploy-s3 deploy-lambda

# Build using the official Lambda Python 3.12 image (no local setup needed)
build-docker:
	rm -rf build function.zip
	docker run --rm --entrypoint /bin/sh -v "$$PWD":/var/task -w /var/task public.ecr.aws/lambda/python:3.12 -lc "\
		python -m pip install -r requirements.txt -t build && \
		cp -r *.py willa_rest_api willa_admin_agent build && \
		cd build && python -m zipfile -c ../function.zip . \
	"

deploy-docker-beta: build-docker deploy-s3-beta deploy-lambda-beta
deploy-docker-prod: build-docker deploy-s3-prod deploy-lambda-prod