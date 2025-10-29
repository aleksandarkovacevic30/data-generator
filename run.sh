# 1) Put your GLEIF CSV at: ./data/gleif_entities.csv
#    (Or change configs/demo.yaml → gleif_input)
# 2) Create batch files for Scenario 1 + 2
python -m gleif_demo make-batch --config configs/demo.yaml

# 3) Start the live stream for Scenario 3 (leave running during the demo)
python -m gleif_demo stream --config configs/demo.yaml
