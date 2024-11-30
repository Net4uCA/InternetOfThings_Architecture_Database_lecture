from typing import Dict, List, Optional
from datetime import datetime
from src.services.base import BaseService


# This service should be placed in src/services/temperature_prediction.py

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

        # Get all rooms and the target bottle from digital replicas
        rooms = [dr for dr in data['digital_replicas'] if dr['type'] == 'room']
        bottles = [dr for dr in data['digital_replicas'] if dr['type'] == 'bottle']

        if not rooms:
            raise ValueError("No rooms available for analysis")

        # Find the target bottle
        target_bottle = next((b for b in bottles if b['_id'] == bottle_id), None)
        if not target_bottle:
            raise ValueError(f"Bottle {bottle_id} not found")

        # Validate bottle has required data
        if 'data' not in target_bottle or 'optimal_temperature' not in target_bottle['profile']:
            raise ValueError("Bottle does not have optimal temperature specified")

        optimal_temp = target_bottle['profile']['optimal_temperature']

        # Calculate weighted scores for each room
        room_scores = self._calculate_room_scores(rooms, optimal_temp)

        # Sort rooms by score (highest first)
        room_scores.sort(key=lambda x: x['score'], reverse=True)

        # Get best room (if any rooms were scored)
        best_room = room_scores[0] if room_scores else None

        return {
            'bottle_id': bottle_id,
            'bottle_name': target_bottle['profile']['wine_name'],
            'optimal_temperature': optimal_temp,
            'best_room': best_room,
            'all_room_scores': room_scores,
            'timestamp': datetime.utcnow().isoformat()
        }

    def _get_latest_temperature(self, room: Dict) -> Optional[float]:
        """
        Get the latest temperature measurement from a room

        Args:
            room: Room digital replica

        Returns:
            Latest temperature value or None if no temperature measurements exist
        """
        if 'data' not in room or 'measurements' not in room['data']:
            return None

        # Filter temperature measurements and sort by timestamp (newest first)
        temp_measurements = [
            m for m in room['data']['measurements']
            if m['measure_type'] == 'temperature'
        ]

        if not temp_measurements:
            return None

        # Sort by timestamp and get the latest
        sorted_measurements = sorted(
            temp_measurements,
            key=lambda x: datetime.fromisoformat(x['timestamp'].replace(" GMT", "")) if isinstance(x['timestamp'],
                                                                                                   str) else x[
                'timestamp'],
            reverse=True
        )

        return sorted_measurements[0]['value']

    def _calculate_room_scores(self, rooms: List[Dict], optimal_temp: float) -> List[Dict]:
        """
        Calculate temperature scores for each room

        Args:
            rooms: List of room digital replicas
            optimal_temp: Target optimal temperature

        Returns:
            List of room scores with detailed metrics
        """
        room_scores = []

        for room in rooms:
            # Get the latest temperature measurement
            current_temp = self._get_latest_temperature(room)

            # Skip rooms without temperature data
            if current_temp is None:
                continue

            # Calculate base temperature difference
            temp_diff = abs(current_temp - optimal_temp)

            # Calculate normalized score (1 is perfect, 0 is worst)
            # Using exponential decay for more dramatic scoring
            score = 1 / (1 + temp_diff * 0.5)  # Adjusted multiplier for smoother decay

            # Add occupancy penalty if room is crowded
            bottles = room.get('data', {}).get('bottles', [])
            if len(bottles) > 0:
                occupancy_penalty = min(len(bottles) * 0.1, 0.5)  # Max 50% penalty
                score *= (1 - occupancy_penalty)

            room_scores.append({
                'room_id': room['_id'],
                'room_name': room['profile']['name'],
                'current_temperature': current_temp,
                'temperature_difference': round(temp_diff, 2),
                'score': round(score, 3),
                'current_occupancy': len(bottles),
                'metadata': {
                    'floor': room['profile'].get('floor'),
                    'last_updated': room['metadata']['updated_at']
                }
            })

        return room_scores