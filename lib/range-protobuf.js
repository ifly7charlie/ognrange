{
  "nested": {
    "OnglideRangeMessage": {
      "fields": {
        "station": {
          "type": "string",
          "id": 1
        },
        "start": {
          "type": "string",
          "id": 2
        },
        "end": {
          "type": "string",
          "id": 3
        },
        "type": {
          "type": "string",
          "id": 4
        },
        "count": {
          "type": "uint32",
          "id": 5
        },
        "h3s": {
          "type": "bytes",
          "id": 10
        },
        "values": {
          "type": "bytes",
          "id": 11
        },
        "stationmeta": {
          "type": "StationMeta",
          "id": 20
        }
      }
    },
    "StationMeta": {
      "fields": {
        "lat": {
          "type": "float",
          "id": 1
        },
        "lng": {
          "type": "float",
          "id": 2
        },
        "status": {
          "type": "string",
          "id": 3
        }
      }
    }
  }
}