import uuid
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
import random
from faker import Faker
import pyarrow as pa
import argparse

class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_path',
            help='Path to input file in GCS',
        )
        parser.add_argument(
            '--output_base_path',
            help='Base path for output files in GCS',
        )

class GenerateCluster(beam.DoFn):
    def setup(self):
        self.fake = Faker()
        self.label_types = [
            "Audience", "Event", "Interest", "SCD", "Topic",
            "AgeGroup", "Brand", "Gender", "Organisation", 
            "Sector", "Location"
        ]
        self.predicates = [
            "IN_SECTOR",
            "PART_OF",
            "AO_LINKED",
            "OWNED_BY",
            "SCD_LINKED"
        ]

    def process(self, element):
        # Generate cluster of nodes (4-7 nodes)
        cluster_size = random.randint(4, 7)
        nodes = []

        # Generate nodes
        for _ in range(cluster_size):
            # Generate random labels
            num_labels = random.randint(1, 2)
            selected_labels = random.sample(self.label_types, num_labels)
            selected_labels.append("Node")
            label_string = ":".join(selected_labels)

            # Create node
            node = {
                "name": self.fake.company(),
                "labels": label_string,
                "id": str(uuid.uuid4())
            }
            nodes.append(node)

        # Generate edges (incremental connections)
        edges = []
        for i in range(len(nodes) - 1):
            for j in range(i + 1, len(nodes)):
                edge = {
                    "source_id": nodes[i]['id'],
                    "target_id": nodes[j]['id'],
                    "predicate": random.choice(self.predicates),
                    "count": random.randint(100, 6000)
                }
                edges.append(edge)

        yield beam.pvalue.TaggedOutput('nodes', nodes)
        yield beam.pvalue.TaggedOutput('edges', edges)

def run():
    pipeline_options = CustomPipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    input_path = pipeline_options.input_path
    output_base_path = pipeline_options.output_base_path

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Generate clusters of nodes and edges
        clusters = (pipeline 
                | 'Read Dummy file' >> beam.io.ReadFromText(input_path)
                | 'Generate clusters' >> beam.ParDo(GenerateCluster()).with_outputs('nodes', 'edges'))

        nodes = (clusters.nodes 
                | 'Flatten nodes' >> beam.FlatMap(lambda x: x)
                | 'Reshuffle nodes' >> beam.Reshuffle())

        edges = (clusters.edges 
                | 'Flatten edges' >> beam.FlatMap(lambda x: x)
                | 'Reshuffle edges' >> beam.Reshuffle())

        nodes | 'Write nodes' >> beam.io.WriteToParquet(
            f'{output_base_path}/nodes/data',
            schema=pa.schema([
                ('name', pa.string()),
                ('labels', pa.string()),
                ('id', pa.string())
            ]),
            file_name_suffix='.parquet'
        )

        edges | 'Write edges' >> beam.io.WriteToParquet(
            f'{output_base_path}/edges/data',
            schema=pa.schema([
                ('source_id', pa.string()),
                ('target_id', pa.string()),
                ('predicate', pa.string()),
                ('count', pa.int64())
            ]),
            file_name_suffix='.parquet'
        )

if __name__ == '__main__':
    run()
