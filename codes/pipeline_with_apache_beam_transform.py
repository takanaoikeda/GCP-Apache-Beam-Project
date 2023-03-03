import argparse
import json
import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# サービスアカウント　キーパス
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/ea9e05an1d7.json"
INPUT_SUBSCRIPTION = "projects/PROJECT_ID/subscriptions/SUBSC_NAME"
BIGQUERY_TABLE = "PROJECT_ID:DATASET_NAME.TABLE_NAME"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"


class CustomParsing(beam.DoFn):
    """ 変換を適用する """

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """
        タイムスタンプを追加する単純な処理
        """
        parsed = json.loads(element.decode("utf-8"))
        parsed["timestamp"] = timestamp.to_rfc3339()
        yield parsed


def run():
    # 引数
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='PubSubのsubscription"',
        default=INPUT_SUBSCRIPTION,
    )
    parser.add_argument(
        "--output_table", help="出力するBigQueryのテーブル", default=BIGQUERY_TABLE
    )
    parser.add_argument(
        "--output_schema",
        help="BigQueryのスキーマ",
        default=BIGQUERY_SCHEMA,
    )
    known_args, pipeline_args = parser.parse_known_args()

    #pipeline オプション
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    # ステップ定義
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()