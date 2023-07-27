from dbt_artifacts_ext.converter import Converter, ConvertFormat


class ConverterFactory:
    @staticmethod
    def get(convert_format: ConvertFormat) -> Converter:
        raise ValueError(f"No converter available for format '{convert_format}'")
