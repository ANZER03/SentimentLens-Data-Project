setup:
	pip install -r requirements.txt

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	pytest tests/

lint:
	flake8 src/
	black --check src/

format:
	black src/
	isort src/

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache .mypy_cache .venv
