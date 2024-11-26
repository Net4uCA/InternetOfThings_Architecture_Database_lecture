from src.services.base import BaseService
from typing import Dict, List, Optional
import math


class TemperaturePredictionService(BaseService):
    """Service to predict the best room for a bottle based on temperature requirements"""

    def __init__(self):
        self.name = "TemperaturePredictionService"

    def execute(self, data: Dict, **kwargs) -> Dict:
        """
        Execute temperature prediction for a bottle.

        Args:
            data: Dictionary containing digital replicas data
            kwargs: Must include 'bottle_id' to analyze

        Returns:
            Dict containing best room prediction and scoring for all rooms
        """
        bottle_id = kwargs.get('bottle_id')
        if not bottle_id:
            raise ValueError("bottle_id is required")

        print("\n=== Debug: TemperaturePredictionService ===")
        print(f"Looking for bottle_id: {bottle_id}")
        print(f"Number of digital replicas: {len(data['digital_replicas'])}")
        print("\nAll Digital Replicas:")
        for dr in data['digital_replicas']:
            print(f"Type: {dr.get('type')}, ID: {dr.get('_id')}")

        # Get all rooms and the target bottle from digital replicas
        rooms = [dr for dr in data['digital_replicas'] if dr['type'] == 'room']
        bottles = [dr for dr in data['digital_replicas'] if dr['type'] == 'bottle']

        print(f"\nFound {len(rooms)} rooms and {len(bottles)} bottles")
        print("\nAll Bottles:")
        for bottle in bottles:
            print(f"Bottle ID: {bottle.get('_id')}")

        # Find the target bottle
        target_bottle = next((b for b in bottles if b['_id'] == bottle_id), None)

        if not target_bottle:
            print(f"\nERROR: Bottle {bottle_id} not found in digital replicas!")
            print("Available bottle IDs:", [b['_id'] for b in bottles])
            raise ValueError(f"Bottle {bottle_id} not found")

        print(f"\nTarget Bottle found:")
        print(f"Bottle ID: {target_bottle['_id']}")
        print(f"Bottle Profile: {target_bottle.get('profile', {})}")
        print(f"Bottle Data: {target_bottle.get('data', {})}")

        # Get optimal temperature for the bottle
        optimal_temp = target_bottle.get('data', {}).get('optimal_temperature')
        if optimal_temp is None:
            raise ValueError("Bottle does not have optimal temperature specified")

        print(f"\nOptimal Temperature: {optimal_temp}")
        print("\nAvailable Rooms:")
        for room in rooms:
            print(f"Room {room['_id']}: Temperature = {room.get('data', {}).get('temperature')}")

        # Calculate scores for each room
        room_scores = []
        for room in rooms:
            current_temp = room.get('data', {}).get('temperature')
            if current_temp is not None:
                # Calculate temperature difference (lower is better)
                temp_diff = abs(current_temp - optimal_temp)
                score = 1 / (1 + temp_diff)  # Normalize score between 0 and 1

                room_scores.append({
                    'room_id': room['_id'],
                    'room_name': room['profile']['name'],
                    'current_temperature': current_temp,
                    'temperature_difference': temp_diff,
                    'score': score
                })

        # Sort rooms by score (highest first)
        room_scores.sort(key=lambda x: x['score'], reverse=True)

        # Get best room (if any rooms were scored)
        best_room = room_scores[0] if room_scores else None

        result = {
            'bottle_id': bottle_id,
            'bottle_name': target_bottle['profile']['name'],
            'optimal_temperature': optimal_temp,
            'best_room': best_room,
            'all_room_scores': room_scores
        }

        print("\n=== Result ===")
        print(result)

        return result