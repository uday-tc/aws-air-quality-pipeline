.PHONY: install run run-api test dashboard clean

install:
	pip install -r requirements.txt && pip install -e .

run:
	PYTHONPATH=src python -m pipeline.run_pipeline --source sample

run-api:
	PYTHONPATH=src python -m pipeline.run_pipeline --source api

test:
	pytest -v

dashboard:
	streamlit run dashboard/app.py

clean:
	rm -rf data
