BASE_URL ?= http://localhost:8000
up:    ; docker compose up --build -d
down:  ; docker compose down -v
burst: ; python3 burst.py $(BASE_URL)
