"""
Data Extractors
Modules for extracting data from various marketing platforms.
"""

from .base_extractor import BaseExtractor
from .crm_extractor import CRMExtractor
from .meta_ads_extractor import MetaAdsExtractor
from .google_ads_extractor import GoogleAdsExtractor
from .ga4_extractor import GA4Extractor
from .email_platform_extractor import EmailPlatformExtractor

__all__ = [
    "BaseExtractor",
    "CRMExtractor",
    "MetaAdsExtractor",
    "GoogleAdsExtractor",
    "GA4Extractor",
    "EmailPlatformExtractor",
]
