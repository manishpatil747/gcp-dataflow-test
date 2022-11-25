import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import re
import logging
import argparse

# input_file = "F:/gcp/data_files/movies_data.csv"
# output_file = "F:/gcp/data_files/output.txt"



def Main(argv=None):
    parser= argparse.ArgumentParser()
    parser.add_argument('--input',dest='input_file',help='File to read in.')
    parser.add_argument('--output',dest='output_file',help='File to write in.')
    parser.add_argument('--runner',dest='runner',help='Add beam runner')

    known_args, pipeline_args = parser.parse_known_args(argv)

    options=PipelineOptions(pipeline_args)
    p=beam.Pipeline(options=options,runner=known_args.runner)

    pipeline=(p
               | "Read From CSV" >> beam.io.ReadFromText(known_args.input_file)
               | "Clean data" >>beam.Map( lambda x: re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', x))
               | "Write to GCS" >> beam.io.WriteToText(known_args.output_file))

    p.run().wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  Main()