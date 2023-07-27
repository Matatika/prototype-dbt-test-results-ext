from dbt_artifacts_ext.converter import Converter, ConvertFormat
from dbt_artifacts_ext.converter.mermaid import MermaidConverter


class ConverterFactory:
    @staticmethod
    def get(convert_format: ConvertFormat) -> Converter:
        if convert_format in [
            ConvertFormat.MERMAID,
            ConvertFormat.MERMAID_10,
        ]:
            return MermaidConverter()

        raise ValueError(f"No converter available for format: {convert_format}")
