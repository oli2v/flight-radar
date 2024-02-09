start:
	docker compose up -d

run:
	docker exec -it flight-radar-spark-worker-1 spark-submit --master spark://spark-master:7077 run.py

shutdown:
	docker compose down

remove:
	docker compose down --remove-orphans --volumes --rmi all