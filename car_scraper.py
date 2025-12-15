"""
Swedish Used Car Price Scraper with Kafka Producer
Uses blocket_api Python package to fetch car listings and stream to Kafka
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from blocket_api import (
    BlocketAPI,
    CarAd,
    CarSortOrder,
    Location,
    CarModel,
    CarTransmission,
    CarFuelType,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BlocketCarScraper:
    """Scraper for Blocket.se used car listings using official API wrapper"""
    
    def __init__(self):
        self.api = BlocketAPI()
        logger.info("Initialized Blocket API")
    
    def search_cars(
        self,
        query: Optional[str] = None,
        locations: List[Location] = None,
        price_from: Optional[int] = None,
        price_to: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        mileage_from: Optional[int] = None,
        mileage_to: Optional[int] = None,
        sort_order: CarSortOrder = CarSortOrder.PRICE_ASC,
        max_results: int = 100
    ) -> List[Dict]:
        """
        Search for cars with specified criteria
        
        Args:
            query: Optional search query (e.g., "Volvo V70")
            locations: List of Location enums (e.g., [Location.STOCKHOLM])
            price_from: Minimum price in SEK
            price_to: Maximum price in SEK
            year_from: Minimum year
            year_to: Maximum year
            mileage_from: Minimum mileage in km
            mileage_to: Maximum mileage in km
            sort_order: How to sort results
            max_results: Maximum number of results to fetch
        """
        try:
            logger.info(f"Searching cars: query={query}, locations={locations}")
            
            # Search using the API
            results = self.api.search_car(
                query=query,
                locations=locations or [Location.STOCKHOLM],
                price_from=price_from,
                price_to=price_to,
                year_from=year_from,
                year_to=year_to,
                mileage_from=mileage_from,
                mileage_to=mileage_to,
                sort_order=sort_order,
            )
            
            # Convert API results to our format
            listings = []
            for idx, ad in enumerate(results):
                if idx >= max_results:
                    break
                    
                try:
                    listing = self._parse_ad(ad)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    logger.error(f"Error parsing ad: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(listings)} car listings")
            return listings
            
        except Exception as e:
            logger.error(f"Error searching cars: {e}")
            return []
    
    def get_ad_details(self, ad_id: int) -> Optional[Dict]:
        """Fetch detailed information for a specific ad"""
        try:
            logger.info(f"Fetching details for ad {ad_id}")
            ad_details = self.api.get_ad(CarAd(ad_id))
            return self._parse_ad_details(ad_details)
        except Exception as e:
            logger.error(f"Error fetching ad details for {ad_id}: {e}")
            return None
    
    def _parse_ad(self, ad) -> Optional[Dict]:
        """Parse ad from search results into standardized format"""
        try:
            listing = {
                'listing_id': str(ad.ad_id),
                'title': getattr(ad, 'subject', 'Unknown'),
                'price': getattr(ad, 'price', {}).get('value') if hasattr(ad, 'price') else None,
                'year': getattr(ad, 'year', None),
                'mileage': getattr(ad, 'mileage', None),
                'location': self._extract_location(ad),
                'url': f"https://www.blocket.se/{ad.ad_id}",
                'image_url': self._extract_image(ad),
                'scraped_at': datetime.utcnow().isoformat(),
                'source': 'blocket',
                'category': 'car'
            }
            
            # Add car-specific fields if available
            if hasattr(ad, 'make'):
                listing['make'] = ad.make
            if hasattr(ad, 'model'):
                listing['model'] = ad.model
            if hasattr(ad, 'fuel_type'):
                listing['fuel_type'] = ad.fuel_type
            if hasattr(ad, 'transmission'):
                listing['transmission'] = ad.transmission
            
            return listing
            
        except Exception as e:
            logger.error(f"Error parsing ad: {e}")
            return None
    
    def _parse_ad_details(self, ad_details) -> Optional[Dict]:
        """Parse detailed ad information"""
        try:
            details = {
                'listing_id': str(ad_details.ad_id),
                'title': getattr(ad_details, 'subject', 'Unknown'),
                'description': getattr(ad_details, 'body', ''),
                'price': getattr(ad_details, 'price', {}).get('value') if hasattr(ad_details, 'price') else None,
                'year': getattr(ad_details, 'year', None),
                'mileage': getattr(ad_details, 'mileage', None),
                'location': self._extract_location(ad_details),
                'url': f"https://www.blocket.se/{ad_details.ad_id}",
                'images': self._extract_all_images(ad_details),
                'posted_date': getattr(ad_details, 'list_time', None),
                'scraped_at': datetime.utcnow().isoformat(),
                'source': 'blocket',
                'category': 'car'
            }
            
            # Car-specific details
            if hasattr(ad_details, 'make'):
                details['make'] = ad_details.make
            if hasattr(ad_details, 'model'):
                details['model'] = ad_details.model
            if hasattr(ad_details, 'fuel_type'):
                details['fuel_type'] = ad_details.fuel_type
            if hasattr(ad_details, 'transmission'):
                details['transmission'] = ad_details.transmission
            if hasattr(ad_details, 'body_type'):
                details['body_type'] = ad_details.body_type
            if hasattr(ad_details, 'color'):
                details['color'] = ad_details.color
            if hasattr(ad_details, 'engine_power'):
                details['engine_power'] = ad_details.engine_power
            
            return details
            
        except Exception as e:
            logger.error(f"Error parsing ad details: {e}")
            return None
    
    def _extract_location(self, ad) -> str:
        """Extract location from ad"""
        if hasattr(ad, 'location') and ad.location:
            return ad.location.get('name', 'Unknown')
        return 'Unknown'
    
    def _extract_image(self, ad) -> Optional[str]:
        """Extract first image URL from ad"""
        if hasattr(ad, 'images') and ad.images:
            return ad.images[0].get('url') if ad.images else None
        return None
    
    def _extract_all_images(self, ad) -> List[str]:
        """Extract all image URLs from ad"""
        if hasattr(ad, 'images') and ad.images:
            return [img.get('url') for img in ad.images if img.get('url')]
        return []


class CarDataKafkaProducer:
    """Kafka producer for streaming car listing data"""
    
    def __init__(self, bootstrap_servers: List[str] = None, topic: str = 'raw-car-listings'):
        self.bootstrap_servers = bootstrap_servers or ['localhost:9092']
        self.topic = topic
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except KafkaError as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} - Failed to connect to Kafka: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    raise
    
    def send_listing(self, listing: Dict) -> bool:
        """Send a single listing to Kafka"""
        try:
            key = listing.get('listing_id', 'unknown')
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=listing
            )
            
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Sent listing {key} to {record_metadata.topic} "
                        f"partition {record_metadata.partition} "
                        f"offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send listing to Kafka: {e}")
            return False
    
    def send_batch(self, listings: List[Dict]) -> int:
        """Send multiple listings to Kafka"""
        success_count = 0
        for listing in listings:
            if self.send_listing(listing):
                success_count += 1
        
        self.producer.flush()
        logger.info(f"Successfully sent {success_count}/{len(listings)} listings to Kafka")
        return success_count
    
    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main scraping pipeline"""
    logger.info("Starting Swedish car price scraper")
    
    # Initialize scraper
    scraper = BlocketCarScraper()
    
    # Initialize Kafka producer
    try:
        kafka_producer = CarDataKafkaProducer(
            bootstrap_servers=['localhost:9092'],
            topic='raw-car-listings'
        )
    except Exception as e:
        logger.error(f"Could not initialize Kafka producer: {e}")
        logger.info("Running in dry-run mode (no Kafka)")
        kafka_producer = None
    
    # Search for cars - multiple searches to capture different segments
    search_configs = [
        {
            'name': 'All Stockholm Cars',
            'locations': [Location.STOCKHOLM],
            'sort_order': CarSortOrder.LATEST,
            'max_results': 50
        },
        {
            'name': 'Potential Classics (15-30 years old)',
            'locations': [Location.STOCKHOLM, Location.UPPSALA, Location.GOTEBORG],
            'year_from': 1995,
            'year_to': 2010,
            'sort_order': CarSortOrder.PRICE_ASC,
            'max_results': 100
        },
        {
            'name': 'Budget Cars Under 50k',
            'locations': [Location.STOCKHOLM],
            'price_to': 50000,
            'sort_order': CarSortOrder.PRICE_ASC,
            'max_results': 50
        },
    ]
    
    all_listings = []
    
    for config in search_configs:
        logger.info(f"Running search: {config['name']}")
        
        # Extract search params
        search_params = {k: v for k, v in config.items() if k not in ['name', 'max_results']}
        max_results = config.get('max_results', 50)
        
        listings = scraper.search_cars(**search_params, max_results=max_results)
        
        if listings:
            all_listings.extend(listings)
            
            # Stream to Kafka
            if kafka_producer:
                kafka_producer.send_batch(listings)
            else:
                logger.info(f"DRY RUN - Would send {len(listings)} listings to Kafka")
                for listing in listings[:3]:  # Show first 3
                    logger.info(f"Sample: {listing['title']} - {listing['price']} SEK ({listing['year']})")
        
        # Be respectful with delays
        time.sleep(2)
    
    logger.info(f"Scraping complete. Total listings collected: {len(all_listings)}")
    
    # Show some statistics
    if all_listings:
        avg_price = sum(l['price'] for l in all_listings if l.get('price')) / len([l for l in all_listings if l.get('price')])
        logger.info(f"Average price: {avg_price:.0f} SEK")
        
        years = [l['year'] for l in all_listings if l.get('year')]
        if years:
            logger.info(f"Year range: {min(years)} - {max(years)}")
    
    # Cleanup
    if kafka_producer:
        kafka_producer.close()


if __name__ == "__main__":
    main()
    