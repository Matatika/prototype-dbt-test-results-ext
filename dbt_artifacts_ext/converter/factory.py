from dbt_artifacts_ext.converter import Converter, ConvertFormat
from dbt_artifacts_ext.converter.matatika import MatatikaConverter
from dbt_artifacts_ext.converter.mermaid import MermaidConverter


class ConverterFactory:
    @staticmethod
    def get(convert_format: ConvertFormat) -> Converter:
        if convert_format in [
            ConvertFormat.MERMAID,
            ConvertFormat.MERMAID_10,
        ]:
            return MermaidConverter()

        if convert_format in [
            ConvertFormat.MATATIKA,
            ConvertFormat.MATATIKA_0,
        ]:
            return MatatikaConverter()

        raise ValueError(f"No converter available for format: {convert_format}")
