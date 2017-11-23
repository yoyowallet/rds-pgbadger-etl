.PHONY: all
all: deps

.PHONY: deps
deps: requirements.txt
	pip install --requirement requirements.txt

.PHONY: clean
clean:
	find . -regex "\(.*__pycache__.*\|*.py[co]\)" -delete

requirements.txt: requirements.in
	pip install --upgrade pip-tools
	pip-compile --output-file $@ $<
