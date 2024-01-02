from common import *
from load_data import DataLoader

if __name__ == "__main__":
    # Usage with arguments
    parser = argparse.ArgumentParser(description='Run forecasting script.')
    parser.add_argument('--config_path', type=str, help='Path to the config file (config.yml)', default=r'config.yml')    
    args = parser.parse_args()

    # Load YAML configuration
    config_path = args.config_path
    try:
        with open(config_path, 'r') as config_file:
            config_data = yaml.safe_load(config_file)
        config = DataLoaderConfig(**config_data)
    except ValidationError as e:
        print(f"Invalid configuration: {e}")
        config = None

    if config:
        # Create DataLoader instance with validated configuration
        data_loader = DataLoader(config)
        data_loader.load_data_save_to_parquet()