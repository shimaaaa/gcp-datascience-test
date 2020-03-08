import apache_beam as beam
import csv
import datetime
from apache_beam.options.pipeline_options import PipelineOptions


class Options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            default='./test.csv',
            help='Input path for the pipeline')

        parser.add_argument(
            '--output',
            default='./output.csv',
            help='Output path for the pipeline')


class FillNullConsumption(beam.DoFn):
    def process(self, element, *args, **kwargs):
        el = list(element)
        if el[2] == 'Null':
            el[2] = '0.0'
        yield el


class WriteToCSV(beam.DoFn):
    def process(self, element, *args, **kwargs):
        yield ','.join(element[0])


class CreateGroupKey(beam.DoFn):
    def process(self, element, *args, **kwargs):
        key = f"{element[0]}__{element[1].split(' ')[0]}"
        yield [key, element]


class FlattenConsumptionData(beam.DoFn):
    def _create_header(self):
        frames = [f'Frame_{i}' for i in range(1, 49)]
        headers = [
            'LCLid',
            'Date'
        ]
        headers.extend(frames)
        return headers

    def _create_data(self, element):
        key = element[0]
        data = element[1]
        element_date = key.split('__')[1]
        element_datetime_from = datetime.datetime.strptime(element_date, '%Y-%m-%d')
        element_datetime_to = element_datetime_from + datetime.timedelta(days=1)
        target = element_datetime_from

        flatten_data = [
            data[0][0],
            element_date
        ]
        while (True):
            if target >= element_datetime_to:
                break
            target_str = target.strftime("%Y-%m-%d %H:%M:%S.0000000")
            has_data = False
            for d in data:
                if d[1] == target_str:
                    has_data = True
                    flatten_data.append(d[2].strip())
                    break
            if not has_data:
                flatten_data.append('0.0')
            target = target + datetime.timedelta(minutes=30)
        return flatten_data

    def process(self, element, *args, **kwargs):
        if element[0] == 'LCLid__DateTime':
            yield [self._create_header()]
        else:
            yield [self._create_data(element)]


def main():
    options = Options()

    with beam.Pipeline(options=options) as pipeline:

        rows = (
            pipeline
            | 'ReadFromText' >> beam.io.ReadFromText(options.input)
            | 'ReadCSV' >> beam.Map(lambda line: next(csv.reader([line])))
        )
        transformed = (
            rows
            | 'SelectField' >> beam.Map(lambda fields: (fields[0], fields[2], fields[3]))
            | 'FillNullConsumption' >> beam.ParDo(FillNullConsumption())
            | 'CreateGroupKey' >> beam.ParDo(CreateGroupKey())
            | 'GroupByKey' >> beam.GroupByKey()
            | 'FlattenConsumptionData' >> beam.ParDo(FlattenConsumptionData())
            | 'ConvertCsv' >> beam.ParDo(WriteToCSV())
        )
        (
            transformed
            | 'Output' >> beam.io.WriteToText(options.output)
        )


if __name__ == '__main__':
    main()
