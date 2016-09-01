#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word length example using R libraries
"""

from __future__ import absolute_import

import argparse
import logging
import re
import pip

import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions

# Install rpy2 and required libraries
# It is necessary to install rpy2 at this stage rather than in setup.py because it seems dataflow installs the
# Python packages prior to running the custom commands.  rpy2 requires R to be accessible from the commandline at
# the time of install.
pip.main(['install', 'rpy2'])
from rpy2.robjects import r
r('if (!require(stringr)) {install.packages("stringr", repos="http://cran.cnr.berkeley.edu/"); library(stringr)}')

empty_line_aggregator = beam.Aggregator('emptyLines')
average_word_size_aggregator = beam.Aggregator('averageWordLength',
                                               beam.combiners.MeanCombineFn(),
                                               float)


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, context):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      context: the call-specific context: data and aggregator.
    Returns:
      The processed element.
    """
    text_line = context.element.strip()
    if not text_line:
      context.aggregate_to(empty_line_aggregator, 1)
    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      context.aggregate_to(average_word_size_aggregator, len(w))
    return words


class WordLengthDoFn(beam.DoFn):
  """Determine the length of each word. """
  def process(self, context):

    """Returns a key-value pair with the length of the element.
      The element is a word.
      Args:
        context: the word to be measured
      Returns:
        A key-value pair with the element as the key and the length of the element as the value.
    """

    word = context.element.strip()
    #length = r('nchar("%s")' % word)
    length = r('str_length("%s")' % word)
    yield '%s: %s' % (word, length[0])



def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')


  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

#####
  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> beam.io.Read(beam.io.TextFileSource(known_args.input))

  # Determine the length of each word
  counts = (lines
            | 'extract' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(unicode))
            | 'length' >> beam.ParDo(WordLengthDoFn()))

  counts | 'write' >> beam.io.Write(beam.io.TextFileSink(known_args.output))

  # Actually run the pipeline (all operations above are deferred).
  result = p.run()
  empty_line_values = result.aggregated_values(empty_line_aggregator)
  logging.info('number of empty lines: %d', sum(empty_line_values.values()))
  word_length_values = result.aggregated_values(average_word_size_aggregator)
  logging.info('average word lengths: %s', word_length_values.values())



  p.run()

