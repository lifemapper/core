"""Contains functions for validating GeoJSON
"""
import json


# .............................................................................
def validate_geojson(obj_generator):
    """Very simple validator to see if response has a couple keys
    """
    try:
        test_json = json.load(obj_generator)
        assert 'type' in test_json
        assert 'geometry' in test_json
        return True
    except Exception:
        return False
