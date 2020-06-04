
run/%:
	export FLASK_APP=faws.$*.server && poetry run flask run

test/all:
	poetry run pytest tests

test/%:
	poetry run pytest tests/$*
