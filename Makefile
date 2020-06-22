
run/%:
	export FLASK_APP=faws.$*.server && poetry run flask run

test/all:
	poetry run pytest tests
	poetry run pytest tests

test/%:
	poetry run pytest tests/$*

test-cov/all:
	poetry run pytest --cov=faws --cov-report=term-missing --cov-config=.coveragerc

test-cov/%:
	poetry run pytest --cov=faws/$* --cov-report=term-missing --cov-config=.coveragerc

push-codecov:
	poetry run codecov --token $(CODECOV_TOKEN)

format:
	@black faws
	@black tests
