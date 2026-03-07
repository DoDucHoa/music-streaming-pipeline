#!/usr/bin/env python3
"""
Python Event Generator - Compatible with eventsim output
Generates realistic music streaming events with proper length field
"""

import json
import gzip
import random
import time
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import os

from kafka import KafkaProducer
from faker import Faker
import numpy as np


class SongGenerator:
    """Loads and generates songs with duration from listen_counts.txt.gz"""
    
    def __init__(self, data_file: str = "data/listen_counts.txt.gz"):
        self.songs = []  # List of (track_id, artist, song, duration, weight)
        self.weights = []
        self.track_map = {}
        self.similar_songs = defaultdict(list)
        
        print(f"Loading song file: {data_file}", file=sys.stderr)
        self._load_songs(data_file)
        print(f"Loaded {len(self.songs)} songs", file=sys.stderr)
        
        # Try to load similar songs (optional)
        try:
            self._load_similar_songs("data/similar_songs.csv.gz")
        except FileNotFoundError:
            print("Similar songs file not found (optional)", file=sys.stderr)
    
    def _load_songs(self, filename: str):
        """Load songs from compressed tab-separated file"""
        with gzip.open(filename, 'rt', encoding='ISO-8859-1') as f:
            for i, line in enumerate(f):
                if i % 5000 == 0:
                    print(f"\rLoading songs: {i}", end='', file=sys.stderr)
                
                try:
                    parts = line.strip().split('\t')
                    if len(parts) >= 5:
                        track_id = parts[0]
                        artist = parts[1]
                        song_name = parts[2]
                        duration = float(parts[3]) if parts[3] else 180.0
                        count = int(parts[4])
                        
                        self.songs.append((track_id, artist, song_name, duration))
                        self.weights.append(count)
                        self.track_map[track_id] = (artist, song_name, duration)
                except (ValueError, IndexError) as e:
                    continue
        
        print(f"\nLoaded {len(self.songs)} tracks", file=sys.stderr)
    
    def _load_similar_songs(self, filename: str):
        """Load similar songs mapping (optional)"""
        with gzip.open(filename, 'rt', encoding='ISO-8859-1') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    track_id, similar_track = parts[0], parts[1]
                    if similar_track in self.track_map:
                        self.similar_songs[track_id].append(similar_track)
    
    def get_random_song(self, last_track_id: Optional[str] = None) -> Tuple[str, str, str, float]:
        """Get random song, preferring similar songs if available"""
        if last_track_id and last_track_id in self.similar_songs and self.similar_songs[last_track_id]:
            # Pick similar song
            next_track_id = random.choice(self.similar_songs[last_track_id])
            artist, song, duration = self.track_map[next_track_id]
            return (next_track_id, artist, song, duration)
        else:
            # Pick weighted random
            idx = random.choices(range(len(self.songs)), weights=self.weights, k=1)[0]
            return self.songs[idx]


class UserPropertiesGenerator:
    """Generates realistic user properties"""
    
    def __init__(self):
        self.faker = Faker()
        
        # Load locations from data files
        self.locations = self._load_locations()
        self.user_agents = self._load_user_agents()
        
    def _load_locations(self) -> List[Dict]:
        """Load US city locations"""
        # Default locations if file not found
        default_locations = [
            {"city": "New York", "state": "NY", "zip": "10001", "lat": 40.7128, "lon": -74.0060},
            {"city": "Los Angeles", "state": "CA", "zip": "90001", "lat": 34.0522, "lon": -118.2437},
            {"city": "Chicago", "state": "IL", "zip": "60601", "lat": 41.8781, "lon": -87.6298},
            {"city": "Houston", "state": "TX", "zip": "77001", "lat": 29.7604, "lon": -95.3698},
            {"city": "Phoenix", "state": "AZ", "zip": "85001", "lat": 33.4484, "lon": -112.0740},
            {"city": "Philadelphia", "state": "PA", "zip": "19019", "lat": 39.9526, "lon": -75.1652},
            {"city": "San Antonio", "state": "TX", "zip": "78201", "lat": 29.4241, "lon": -98.4936},
            {"city": "San Diego", "state": "CA", "zip": "92101", "lat": 32.7157, "lon": -117.1611},
            {"city": "Dallas", "state": "TX", "zip": "75201", "lat": 32.7767, "lon": -96.7970},
            {"city": "San Jose", "state": "CA", "zip": "95101", "lat": 37.3382, "lon": -121.8863},
        ]
        return default_locations
    
    def _load_user_agents(self) -> List[str]:
        """Load user agent strings"""
        # Just return defaults - file has encoding issues
        return self._default_user_agents()
    
    def _default_user_agents(self) -> List[str]:
        return [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        ]
    
    def generate_user_props(self, user_id: int) -> Dict:
        """Generate user properties"""
        location = random.choice(self.locations)
        gender = random.choice(['M', 'F'])
        
        return {
            "userId": str(user_id),
            "firstName": self.faker.first_name_male() if gender == 'M' else self.faker.first_name_female(),
            "lastName": self.faker.last_name(),
            "gender": gender,
            "location": f"{location['city']}, {location['state']}",
            "city": location['city'],
            "state": location['state'],
            "zip": location['zip'],
            "lat": location['lat'],
            "lon": location['lon'],
            "userAgent": random.choice(self.user_agents),
            "registration": int((datetime.now() - timedelta(days=random.randint(1, 365))).timestamp() * 1000)
        }


class Session:
    """Represents a user session"""
    
    # State transition probabilities (simplified from example-config.json)
    TRANSITIONS = {
        ("Home", "Logged In", "free"): [
            ("Home", 0.1), ("NextSong", 0.6), ("Thumbs Up", 0.1), 
            ("Thumbs Down", 0.05), ("Add to Playlist", 0.05), ("Logout", 0.05)
        ],
        ("Home", "Logged In", "paid"): [
            ("Home", 0.08), ("NextSong", 0.7), ("Thumbs Up", 0.08), 
            ("Thumbs Down", 0.04), ("Add to Playlist", 0.06), ("Logout", 0.02), ("Settings", 0.02)
        ],
        ("Home", "Guest", "free"): [
            ("Home", 0.2), ("About", 0.1), ("Help", 0.1), ("Register", 0.3)
        ],
        ("NextSong", "Logged In", "free"): [
            ("NextSong", 0.8), ("Home", 0.05), ("Thumbs Up", 0.05), 
            ("Thumbs Down", 0.03), ("Add to Playlist", 0.02), ("Logout", 0.02), ("Upgrade", 0.03)
        ],
        ("NextSong", "Logged In", "paid"): [
            ("NextSong", 0.85), ("Home", 0.03), ("Thumbs Up", 0.04), 
            ("Thumbs Down", 0.02), ("Add to Playlist", 0.04), ("Logout", 0.01), ("Settings", 0.01)
        ],
        ("Register", "Guest", "free"): [
            ("Submit Registration", 0.7), ("Home", 0.2)
        ],
        ("About", "Guest", "free"): [
            ("Home", 0.5), ("Register", 0.2), ("Help", 0.1)
        ],
        ("Help", "Guest", "free"): [
            ("Home", 0.4), ("About", 0.1), ("Register", 0.1)
        ],
        ("Submit Registration", "Guest", "free"): [
            ("Home", 1.0)  # Transition to logged in
        ],
        ("Logout", "Logged In", "free"): [
            ("Home", 1.0)  # Session ends, next session will be logged out
        ],
        ("Logout", "Logged In", "paid"): [
            ("Home", 1.0)
        ],
    }
    
    # Starting pages for new sessions
    NEW_SESSION_PAGES = [
        ("Home", "Guest", "free", 100),
        ("Home", "Logged In", "free", 1000),
        ("Home", "Logged In", "paid", 500),
        ("NextSong", "Logged In", "free", 1500),
        ("NextSong", "Logged In", "paid", 1750),
        ("Register", "Guest", "free", 30),
        ("About", "Guest", "free", 20),
    ]
    
    def __init__(self, session_id: int, user_id: int, auth: str, level: str, 
                 start_time: datetime, song_generator: SongGenerator):
        self.session_id = session_id
        self.user_id = user_id
        self.auth = auth
        self.level = level
        self.current_time = start_time
        self.item_in_session = 0
        self.song_generator = song_generator
        self.done = False
        
        # Pick initial page
        pages, weights = [], []
        for page, a, l, w in self.NEW_SESSION_PAGES:
            if a == auth and l == level:
                pages.append(page)
                weights.append(w)
        
        if not pages:
            pages, weights = ["Home"], [1]
        
        self.current_page = random.choices(pages, weights=weights, k=1)[0]
        self.current_song = None
        self.current_song_end = None
        
        if self.current_page == "NextSong":
            track_id, artist, song, duration = self.song_generator.get_random_song()
            self.current_song = (track_id, artist, song, duration)
            self.current_song_end = self.current_time + timedelta(seconds=duration)
    
    def next_event(self) -> Optional[Dict]:
        """Generate next event in session"""
        if self.done:
            return None
        
        # Determine method and status based on page
        if self.current_page == "NextSong":
            method, status = "PUT", 200
        elif self.current_page in ["Login", "Submit Registration", "Logout"]:
            method, status = "PUT", 307
        else:
            method, status = "GET", 200
        
        # Build event
        event = {
            "ts": int(self.current_time.timestamp() * 1000),
            "userId": str(self.user_id) if self.auth in ["Logged In", "Cancelled"] else "",
            "sessionId": self.session_id,
            "page": self.current_page,
            "auth": self.auth,
            "method": method,
            "status": status,
            "level": self.level,
            "itemInSession": self.item_in_session,
        }
        
        # Add song info for NextSong events
        if self.current_page == "NextSong" and self.current_song:
            event["artist"] = self.current_song[1]
            event["song"] = self.current_song[2]
            event["length"] = self.current_song[3]
        
        # Move to next state
        self._transition()
        
        return event
    
    def _transition(self):
        """Transition to next state"""
        self.item_in_session += 1
        
        # Get possible next states
        key = (self.current_page, self.auth, self.level)
        if key in self.TRANSITIONS:
            next_pages, probs = zip(*self.TRANSITIONS[key])
            self.current_page = random.choices(next_pages, weights=probs, k=1)[0]
        else:
            # Default: 80% stay, 20% end session
            if random.random() < 0.2:
                self.done = True
                return
        
        # Handle special pages
        if self.current_page in ["Logout", "Cancel", "Downgrade"]:
            self.done = True
            return
        
        # Handle song transitions
        if self.current_page == "NextSong":
            if self.current_song and self.current_time < self.current_song_end:
                # Continue current song
                self.current_time = self.current_song_end
            
            # Get next song (possibly similar)
            last_track = self.current_song[0] if self.current_song else None
            track_id, artist, song, duration = self.song_generator.get_random_song(last_track)
            self.current_song = (track_id, artist, song, duration)
            self.current_song_end = self.current_time + timedelta(seconds=duration)
        else:
            # Regular page: advance time by random amount (5-60 seconds)
            self.current_time += timedelta(seconds=np.random.exponential(20))
            self.current_song = None
        
        # End session randomly (5% chance after 10 events)
        if self.item_in_session > 10 and random.random() < 0.05:
            self.done = True


class MusicStreamingEventGenerator:
    """Main event generator"""
    
    def __init__(self, num_users: int = 1000, 
                 kafka_brokers: str = "localhost:9092",
                 kafka_topic: str = "listen_events",
                 start_time: Optional[datetime] = None,
                 continuous: bool = True):
        
        self.num_users = num_users
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.continuous = continuous
        self.start_time = start_time or datetime(2024, 1, 1)
        self.current_time = self.start_time
        
        # Initialize generators
        print("Initializing generators...", file=sys.stderr)
        self.song_generator = SongGenerator()
        self.user_props_generator = UserPropertiesGenerator()
        
        # Initialize Kafka producer with retry logic
        print(f"Connecting to Kafka at {kafka_brokers}...", file=sys.stderr)
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(1, max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_brokers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    max_block_ms=5000
                )
                print(f"✅ Successfully connected to Kafka!", file=sys.stderr)
                break
            except Exception as e:
                if attempt < max_retries:
                    print(f"⚠️  Attempt {attempt}/{max_retries} failed: {e}", file=sys.stderr)
                    print(f"⏳ Retrying in {retry_delay} seconds...", file=sys.stderr)
                    time.sleep(retry_delay)
                else:
                    print(f"❌ Failed to connect to Kafka after {max_retries} attempts", file=sys.stderr)
                    raise
        
        # Create users
        print(f"Creating {num_users} users...", file=sys.stderr)
        self.users = self._create_users()
        
        # Session counter
        self.session_counter = 1
        
        print("✅ Event generator ready!", file=sys.stderr)
    
    def _create_users(self) -> List[Dict]:
        """Create user profiles"""
        users = []
        for user_id in range(1, self.num_users + 1):
            user = self.user_props_generator.generate_user_props(user_id)
            
            # Determine level (free vs paid)
            user['level'] = random.choices(['free', 'paid'], weights=[10, 2], k=1)[0]
            
            # Determine auth state (Guest, Logged In, Logged Out)
            user['auth'] = random.choices(['Guest', 'Logged In', 'Logged Out'], 
                                         weights=[1, 10, 2], k=1)[0]
            
            # Session timing parameters (exponential distribution)
            user['alpha'] = 90.0  # Expected time between events (seconds)
            user['beta'] = 604800.0  # Expected time between sessions (seconds, ~7 days)
            user['next_session_time'] = self.start_time + timedelta(
                seconds=random.uniform(0, user['beta'])
            )
            user['current_session'] = None
            
            users.append(user)
        
        return users
    
    def generate_events(self):
        """Main event generation loop"""
        events_generated = 0
        
        try:
            while True:
                # Check which users should start sessions
                for user in self.users:
                    if user['current_session'] is None and self.current_time >= user['next_session_time']:
                        # Start new session
                        user['current_session'] = Session(
                            session_id=self.session_counter,
                            user_id=int(user['userId']),
                            auth=user['auth'],
                            level=user['level'],
                            start_time=self.current_time,
                            song_generator=self.song_generator
                        )
                        self.session_counter += 1
                
                # Generate events from active sessions
                for user in self.users:
                    if user['current_session'] is not None:
                        event = user['current_session'].next_event()
                        
                        if event is not None:
                            # Merge user properties into event (exclude internal fields)
                            exclude_fields = {'current_session', 'next_session_time', 'alpha', 'beta'}
                            
                            if event['auth'] in ['Logged In', 'Cancelled']:
                                # Include all user fields except internal ones
                                full_event = {k: v for k, v in user.items() if k not in exclude_fields}
                                full_event.update(event)
                            else:
                                full_event = {
                                    **event,
                                    "userAgent": user['userAgent'],
                                    "location": user['location'],
                                    "lat": user['lat'],
                                    "lon": user['lon']
                                }
                            
                            # Send to Kafka
                            self.producer.send(self.kafka_topic, value=full_event)
                            events_generated += 1
                            
                            if events_generated % 100 == 0:
                                print(f"Generated {events_generated} events", file=sys.stderr)
                        
                        # Check if session is done
                        if user['current_session'].done:
                            user['current_session'] = None
                            # Schedule next session
                            user['next_session_time'] = self.current_time + timedelta(
                                seconds=np.random.exponential(user['beta'])
                            )
                
                # Advance time
                self.current_time += timedelta(seconds=1)
                
                # In continuous mode, don't wait (generate as fast as possible)
                # In non-continuous mode, could add delays here
                if not self.continuous and self.current_time > datetime(2024, 12, 31):
                    print("Reached end date, stopping", file=sys.stderr)
                    break
                
                # Flush periodically
                if events_generated % 100 == 0:
                    self.producer.flush()
                
        except KeyboardInterrupt:
            print(f"\n✅ Stopping. Generated {events_generated} events total", file=sys.stderr)
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Music Streaming Event Generator")
    parser.add_argument('--nusers', type=int, default=1000, help='Number of users')
    parser.add_argument('--kafka-broker-list', default='kafka:29092', help='Kafka brokers')
    parser.add_argument('--kafka-topic', default='listen_events', help='Kafka topic')
    parser.add_argument('--start-time', help='Start timestamp (ISO format)')
    parser.add_argument('--continuous', action='store_true', default=True, help='Continuous generation')
    
    args = parser.parse_args()
    
    start_time = datetime.fromisoformat(args.start_time.replace('T', ' ')) if args.start_time else None
    
    generator = MusicStreamingEventGenerator(
        num_users=args.nusers,
        kafka_brokers=args.kafka_broker_list,
        kafka_topic=args.kafka_topic,
        start_time=start_time,
        continuous=args.continuous
    )
    
    generator.generate_events()
