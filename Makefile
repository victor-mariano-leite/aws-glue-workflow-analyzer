install:
	@pip install -r requirements/all.txt
	@pip install pre-commit
	@pre-commit install
	@mypy --install-types
	
prepare:
	@black . -v
	@isort .
	@mypy aws_glue_workflow_analyzer
	@pylint aws_glue_workflow_analyzer
	@flake8 aws_glue_workflow_analyzer
	@echo Good to Go!

check:
	@black . -v --check
	@isort . --check
	@mypy aws_glue_workflow_analyzer
	@flake8 aws_glue_workflow_analyzer
	@pylint aws_glue_workflow_analyzer
	@echo Good to Go!

docs:
	@mkdocs build --clean

docs-serve:
	@mkdocs serve

test:
	@pytest --cov aws_glue_workflow_analyzer

test-cov:
	@pytest --cov aws_glue_workflow_analyzer --cov-report xml:coverage.xml
.PHONY: docs
