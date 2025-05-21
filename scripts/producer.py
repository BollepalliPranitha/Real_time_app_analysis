import json
import time
import random
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate data

def generate_live_data():
    return {
        "EventID": random.randint(100, 1000),
        "EventType": "LiveStream",
        "UserID": random.randint(1000, 10000),
        "Platform": random.choice(["Twitch", "YouTube", "Facebook", "twitch"]),
        "LiveEngagement": random.randint(50, 500),
        "ViewerCount": random.randint(100, 5000),
        "StreamDuration": random.randint(-1000, 14400),  # Includes invalid durations
        "DeviceType": random.choice(["Smartphone", "Desktop", "Tablet", None]),
        "WatchTime": random.choice(["Morning", "Afternoon", "Evening", "Night", None]),
        "AddictionLevel": random.randint(1, 5)
    }

def generate_video_data():
    return {
        "UserID": random.randint(1000, 10000),
        "Age": random.randint(-5, 60),
        "Gender": random.choice(["Male", "Female", "Other", None]),
        "Location": random.choice(["New York", "London", "Tokyo", "Sydney"]),
        "Income": random.randint(20000, 100000),
        "Debt": random.choice([True, False]),
        "OwnsProperty": random.choice([True, False]),
        "Profession": random.choice(["Engineer", "Teacher", "Student", "Artist", None]),
        "Demographics": random.choice(["Urban", "Suburban", "Rural"]),
        "Platform": random.choice(["YouTube", "Vimeo", "TikTok", "youtube"]),
        "TotalTimeSpent": random.randint(300, 7200),
        "NumberOfSessions": random.randint(1, 10),
        "VideoID": random.randint(100, 1000),
        "VideoCategory": random.choice(["Education", "Entertainment", "Gaming"]),
        "VideoLength": random.randint(60, 3600),
        "Engagement": random.randint(10, 100),
        "ImportanceScore": random.randint(1, 10),
        "TimeSpentOnVideo": random.randint(30, 3600),
        "NumberOfVideosWatched": random.randint(1, 20),
        "ScrollRate": random.randint(10, 50),
        "Frequency": random.choice(["Daily", "Weekly", "Monthly"]),
        "ProductivityLoss": random.randint(1, 5),
        "Satisfaction": random.randint(1, 10),
        "WatchReason": random.choice(["Learning", "Entertainment", "Relaxation"]),
        "DeviceType": random.choice(["Smartphone", "Desktop", "Tablet"]),
        "OS": random.choice(["Windows", "iOS", "Android"]),
        "WatchTime": random.choice(["Morning", "Afternoon", "Evening", "Night"]),
        "SelfControl": random.randint(1, 10),
        "AddictionLevel": random.randint(1, 5),
        "CurrentActivity": random.choice(["Work", "Study", "Leisure"]),
        "ConnectionType": random.choice(["Wi-Fi", "Mobile", "Wired"])
    }

# Main loop
while True:
    # Generate and send data for all topics
    
    live_data = generate_live_data()
    video_data = generate_video_data()

    
    producer.send('live_streaming', live_data)
    producer.send('video_interactions', video_data)

    producer.flush()
    print("Sent messages to Kafka topics:  live_streaming, video_interactions")
    time.sleep(5)