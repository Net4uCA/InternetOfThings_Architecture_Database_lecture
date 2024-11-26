from random import randint

from flask import Flask
from src.virtualization.digital_replica.dr_factory import DRFactory
from src.services.database_service import DatabaseService
from src.digital_twin.dt_factory import DTFactory
from src.virtualization.digital_replica.schema_registry import SchemaRegistry
from config.config_loader import ConfigLoader
import time
from datetime import datetime
import random
def main():
    # 1. Initialize core components
    print("\n=== Initializing Components ===")
    schema_registry = SchemaRegistry()
    schema_registry.load_schema('bottle', 'src/virtualization/templates/bottle.yaml')
    schema_registry.load_schema('room', 'src/virtualization/templates/room.yaml')

    db_config = ConfigLoader.load_database_config()
    connection_string = ConfigLoader.build_connection_string(db_config)
    db_service = DatabaseService(connection_string=connection_string,
                               db_name=db_config["settings"]["name"],
                               schema_registry=schema_registry)
    db_service.connect()

    dt_factory = DTFactory(db_service, schema_registry)
    bottle_factory = DRFactory('src/virtualization/templates/bottle.yaml')
    room_factory = DRFactory('src/virtualization/templates/room.yaml')

    try:
        # 2. Create Digital Twin
        dt_id = dt_factory.create_dt(
            name="Premium Wine Cellar"+str(randint(1,10000)),
            description="Digital Twin for premium wine storage management"
        )

        # 3. Create Rooms
        rooms_data = [
            {
                "profile": {"name": "Main Cellar", "room_number": "C001", "floor": -1,
                           "description": "Main storage cellar"},
                "data": {"status": "active", "temperature": random.uniform(10, 30), "humidity": 75.0}
            },
            {
                "profile": {"name": "Reserve Room", "room_number": "C002", "floor": -1,
                           "description": "Premium wine reserve storage"},
                "data": {"status": "active", "temperature": random.uniform(10, 30), "humidity": 78.0}
            },
            {
                "profile": {"name": "Tasting Room", "room_number": "T001", "floor": 1,
                           "description": "Wine tasting and temporary storage"},
                "data": {"status": "active", "temperature": random.uniform(10, 30), "humidity": 65.0}
            },
            {
                "profile": {"name": "Aging Cellar", "room_number": "C003", "floor": -2,
                           "description": "Long-term aging cellar"},
                "data": {"status": "active", "temperature": random.uniform(10, 30), "humidity": 80.0}
            }
        ]

        for room_data in rooms_data:
            room = room_factory.create_dr('room', room_data)
            room_id = db_service.save_dr("room", room)
            dt_factory.add_digital_replica(dt_id, "room", room_id)

        # 4. Create and add Bottle
        bottle_data = {
            "profile": {
                "name": "Barolo Riserva 2015",
                "description": "Premium Barolo wine from Piedmont",
                "rfid_tag": "RF123456"
            },
            "data": {
                "status": "active",
                "optimal_temperature": randint(10,20),
                "properties": {"vintage": 2015, "producer": "Antinori", "region": "Piedmont"}
            }
        }

        bottle = bottle_factory.create_dr('bottle', bottle_data)
        bottle_id = db_service.save_dr("bottle", bottle)
        dt_factory.add_digital_replica(dt_id, "bottle", bottle_id)

        # 5. Add and execute Temperature Prediction Service
        dt_factory.add_service(dt_id, "TemperaturePredictionService")
        dt_instance = dt_factory.get_dt_instance(dt_id)
        prediction_result = dt_instance.execute_service(
            "TemperaturePredictionService",
            bottle_id=bottle_id
        )

        # 6. Print Results
        print("\n=== Temperature Prediction Results ===")
        print(f"Bottle: {prediction_result['bottle_name']}")
        print(f"Optimal Temperature: {prediction_result['optimal_temperature']}°C")
        print("\nBest Room Match:")
        best_room = prediction_result['best_room']
        print(f"- Room: {best_room['room_name']}")
        print(f"- Current Temperature: {best_room['current_temperature']}°C")
        print(f"- Score: {best_room['score']:.2f}")

        print("\nAll Room Scores:")
        for room in prediction_result['all_room_scores']:
            print(f"- {room['room_name']}: {room['score']:.2f} " +
                  f"(temp: {room['current_temperature']}°C)")

    finally:
        db_service.disconnect()

if __name__ == "__main__":
    main()