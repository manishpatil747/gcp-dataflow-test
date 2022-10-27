import argparse
import csv
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None):

    inputData=args.inputfile  #'F:/data/Movies_data.csv'
    outputData=args.inputfile #'F:/data/store_data_output.csv'
    options=PipelineOptions(beam_args,runner='DataflowRunner',project='dataflow-test747',
                            job_name='movie-data-job-dly',temp_location='gs://dataflow-temp-456678/',region='us-central1')

    def parseFile(element):
        for line in csv.reader([element],quotechar='"',delimiter=",",quoting=csv.QUOTE_ALL,skipinitialspace=True):
            return line

    def rmSpecialchar(element):
            element[5]=re.sub(r"[$A-Z,*]","",element[5])
            element[6]=re.sub(r"[$A-Z,*]","",element[6])
            element[7] = re.sub(r"[$A-Z,*]", "", element[8])
            element[8] = re.sub(r"[$A-Z,*]", "", element[8])
            return element


    p=beam.Pipeline(options=options)

    (p | 'Input_Data' >> beam.io.ReadFromText(inputData,skip_header_lines=1)\
         | 'Parse' >> beam.Map(parseFile)\
         | 'Remove_Special_Char' >> beam.Map(rmSpecialchar) \
        | 'Write_to_file' >> beam.io.WriteToText(outputData) )


    p.run().wait_until_finish()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputfile', type=str, required=True)
    parser.add_argument('--outputfile',type=str, required=True)
    args, beam_args = parser.parse_known_args()

    logging.getLogger().setLevel(logging.INFO)
    run()




