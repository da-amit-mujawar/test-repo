import json

config_base = "emrconfig.json"
config_file = "config_step_0.json"

with open(config_base) as b:
    base = json.load(b)

with open(config_file) as f:
    config = json.load(f)


r = list()
base = base + config
