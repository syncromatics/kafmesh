build:
	docker build -t testing --target test .

test: build
	mkdir -p artifacts/
	docker run -v $(PWD)/artifacts:/artifacts -v /var/run/docker.sock:/var/run/docker.sock testing
	cd artifacts && curl -s https://codecov.io/bash | bash
