"""
MinIO Configuration & Utilities
Storage & Lakehouse Architecture (Anggota 4)

Menyediakan:
1. Konfigurasi koneksi MinIO (host, port, credentials)
2. S3A configuration untuk Spark
3. Bucket definitions (Bronze, Silver, Gold)
4. Utility functions untuk bucket management

Usage:
    from minio_config import MinIOConfig, get_minio_client
    
    config = MinIOConfig()
    client = get_minio_client(config)
    client.list_buckets()
"""

import os
from typing import Dict, Any, List
from dataclasses import dataclass
from minio import Minio
from minio.commonconfig import GOVERNANCE
from minio.retention import Retention


@dataclass
class MinIOConfig:
    """Konfigurasi koneksi dan parameter MinIO"""
    
    # Koneksi
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    
    # Secure flag (gunakan HTTPS di production)
    use_ssl: bool = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
    
    # Bucket names (sesuai dengan Medallion Architecture)
    bucket_bronze: str = "bronze"
    bucket_silver: str = "silver"
    bucket_gold: str = "gold"
    
    # Path dalam bucket (sesuai dengan Spark output path)
    path_bronze: str = "violations"
    path_silver: str = "violations_clean"
    path_gold: str = "violations_agg"
    
    def get_full_path(self, layer: str) -> str:
        """Get full S3A path untuk layer tertentu
        
        Args:
            layer: 'bronze', 'silver', atau 'gold'
            
        Returns:
            Full S3A path (s3a://bucket/path)
        """
        if layer == "bronze":
            return f"s3a://{self.bucket_bronze}/{self.path_bronze}"
        elif layer == "silver":
            return f"s3a://{self.bucket_silver}/{self.path_silver}"
        elif layer == "gold":
            return f"s3a://{self.bucket_gold}/{self.path_gold}"
        else:
            raise ValueError(f"Unknown layer: {layer}")
    
    def get_spark_hadoop_config(self) -> Dict[str, str]:
        """Get Spark Hadoop configuration untuk S3A
        
        Returns:
            Dictionary of Spark config parameters
            
        Usage:
            config = MinIOConfig()
            spark_config = config.get_spark_hadoop_config()
            
            spark = SparkSession.builder \\
                .config("spark.hadoop.fs.s3a.endpoint", spark_config["fs.s3a.endpoint"]) \\
                .config("spark.hadoop.fs.s3a.access.key", spark_config["fs.s3a.access.key"]) \\
                ... \\
                .getOrCreate()
        """
        endpoint_url = self.endpoint.replace("http://", "").replace("https://", "")
        
        return {
            "fs.s3a.endpoint": self.endpoint,
            "fs.s3a.access.key": self.access_key,
            "fs.s3a.secret.key": self.secret_key,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.connection.ssl.enabled": str(self.use_ssl).lower(),
        }


def get_minio_client(config: MinIOConfig) -> Minio:
    """Create MinIO client
    
    Args:
        config: MinIOConfig instance
        
    Returns:
        Minio client object
    """
    endpoint_url = config.endpoint.replace("http://", "").replace("https://", "")
    
    client = Minio(
        endpoint=endpoint_url,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.use_ssl,
    )
    return client


def ensure_buckets_exist(config: MinIOConfig, verbose: bool = True) -> None:
    """Create buckets jika belum ada
    
    Args:
        config: MinIOConfig instance
        verbose: Print status messages
    """
    client = get_minio_client(config)
    buckets_to_create = [
        config.bucket_bronze,
        config.bucket_silver,
        config.bucket_gold,
    ]
    
    try:
        existing_buckets = [bucket.name for bucket in client.list_buckets()]
        
        for bucket in buckets_to_create:
            if bucket in existing_buckets:
                if verbose:
                    print(f"✓ Bucket '{bucket}' sudah ada")
            else:
                client.make_bucket(bucket)
                if verbose:
                    print(f"✓ Bucket '{bucket}' berhasil dibuat")
    except Exception as e:
        print(f"✗ Error membuat bucket: {e}")
        raise


def list_objects_in_layer(config: MinIOConfig, layer: str) -> List[str]:
    """List semua objects dalam layer tertentu
    
    Args:
        config: MinIOConfig instance
        layer: 'bronze', 'silver', atau 'gold'
        
    Returns:
        List of object names
    """
    client = get_minio_client(config)
    
    if layer == "bronze":
        bucket = config.bucket_bronze
        prefix = config.path_bronze
    elif layer == "silver":
        bucket = config.bucket_silver
        prefix = config.path_silver
    elif layer == "gold":
        bucket = config.bucket_gold
        prefix = config.path_gold
    else:
        raise ValueError(f"Unknown layer: {layer}")
    
    objects = []
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            objects.append(obj.object_name)
    except Exception as e:
        print(f"✗ Error listing objects: {e}")
    
    return objects


def get_bucket_info(config: MinIOConfig) -> Dict[str, Any]:
    """Get informasi lengkap tentang buckets
    
    Returns:
        Dictionary dengan info size dan object count per bucket
    """
    client = get_minio_client(config)
    info = {}
    
    for layer in ["bronze", "silver", "gold"]:
        if layer == "bronze":
            bucket = config.bucket_bronze
            prefix = config.path_bronze
        elif layer == "silver":
            bucket = config.bucket_silver
            prefix = config.path_silver
        else:
            bucket = config.bucket_gold
            prefix = config.path_gold
        
        try:
            objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
            total_size = sum(obj.size for obj in objects if obj.size is not None)
            
            info[layer] = {
                "bucket": bucket,
                "prefix": prefix,
                "object_count": len(objects),
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
            }
        except Exception as e:
            info[layer] = {"error": str(e)}
    
    return info


# ============================================================================
# Quick Access Constants
# ============================================================================

# Default config instance
DEFAULT_CONFIG = MinIOConfig()

# S3A paths untuk digunakan di Spark
BRONZE_S3A_PATH = DEFAULT_CONFIG.get_full_path("bronze")
SILVER_S3A_PATH = DEFAULT_CONFIG.get_full_path("silver")
GOLD_S3A_PATH = DEFAULT_CONFIG.get_full_path("gold")
