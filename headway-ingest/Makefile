VENV := .venv
PY := $(VENV)/bin/python
STAMP := $(VENV)/.installed
SPEED ?= 20
START ?=
END ?=

.PHONY: setup run dry-run inject replay

$(STAMP): requirements.txt
	test -x $(PY) || uv venv $(VENV)
	uv pip install --python $(PY) -r requirements.txt
	touch $(STAMP)

setup: $(STAMP)

run: $(STAMP)
	$(PY) src/producer.py

dry-run: $(STAMP)
	$(PY) src/producer.py --dry-run

inject: $(STAMP)
	$(PY) inject.py

replay: setup
	@test -n "$(START)" || { echo 'usage: make replay START=2026-09-04T07:30 [END=...] [SPEED=20]'; exit 1; }
	$(PY) replay.py --start $(START) $(if $(END),--end $(END)) --speed $(SPEED)
