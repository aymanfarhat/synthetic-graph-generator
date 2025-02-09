# Synthetic Graph Generator
Batch pipeline for generating a synthetic graph 1 billion+ Edges and Nodes. Built in Apache Beam on Dataflow. Input is a PCollection of cluster placeholders, output is a PCollection of graph nodes and edges in Parquet format.

## Run on Dataflow

```bash
python beam_gen_graph.py \
    --name=graph-generation-job \
    --runner=DataflowRunner \
    --input_path=$INPUT_PATH \
    --output_base_path=$OUTPUT_BASE_PATH \
    --project=$PROJECT_ID \
    --region=$REGION \
    --staging_location=$STAGING_LOCATION \
    --temp_location=$TEMP_LOCATION \
    --requirements_file="./requirements.txt" \
    --setup_file="./setup.py" \
    --experiments="use_runner_v2" \
    --experiments="enable_recommendations" \
    --experiments="enable_dynamic_work_rebalancing" \
    --machine_type="n1-standard-2" \
    --disk_size_gb=50 \
    --network=$NETWORK \
    --subnetwork=$SUBNETWORK \
    --no_use_public_ips
```
