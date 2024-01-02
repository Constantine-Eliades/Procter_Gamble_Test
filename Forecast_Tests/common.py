from pydantic import BaseModel, DirectoryPath, FilePath, ValidationError
import pandas as pd
import yaml
import string
import argparse
import os
from typing import List

class DataLoaderConfig(BaseModel):
    data_paths: dict[str, FilePath]
    output_path: DirectoryPath
    partition_by: List[str]