
run/%:
	export FLASK_APP=faws.$*.server && flask run

test/all:
	pytest tests

test/%:
	pytest tests/$*
